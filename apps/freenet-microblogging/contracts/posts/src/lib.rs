// use chrono::{DateTime, Utc};
use ed25519_dalek::Verifier;
use locutus_stdlib::{
    blake2::{Blake2b512, Digest},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[derive(Serialize, Deserialize)]
struct PostsFeed {
    messages: Vec<Post>,
}

impl<'a> TryFrom<State<'a>> for PostsFeed {
    type Error = ContractError;

    fn try_from(value: State<'a>) -> Result<Self, Self::Error> {
        serde_json::from_slice(value.as_ref()).map_err(|_| ContractError::InvalidState)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Post {
    pub author: String,
    // date: DateTime<Utc>,
    pub title: String,
    pub content: String,
    #[serde(default = "Post::modded")]
    pub mod_msg: bool,
    pub signature: Option<ed25519_dalek::Signature>,
}

impl Post {
    pub fn hash(&self) -> [u8; 64] {
        let mut hasher = Blake2b512::new();
        hasher.update(self.author.as_bytes());
        hasher.update(self.title.as_bytes());
        hasher.update(self.content.as_bytes());
        let hash_val = hasher.finalize();
        let mut key = [0; 64];
        key.copy_from_slice(&hash_val[..]);
        key
    }

    pub fn modded() -> bool {
        false
    }
}

#[derive(Serialize, Deserialize)]
struct FeedSummary {
    summaries: Vec<MessageSummary>,
}

impl<'a> From<&'a mut PostsFeed> for FeedSummary {
    fn from(feed: &'a mut PostsFeed) -> Self {
        // feed.messages.sort_by_key(|m| m.date);
        let mut summaries = Vec::with_capacity(feed.messages.len());
        for msg in &feed.messages {
            summaries.push(MessageSummary(msg.hash()));
        }
        FeedSummary { summaries }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct MessageSummary(#[serde_as(as = "[_; 64]")] [u8; 64]);

impl<'a> TryFrom<StateSummary<'a>> for MessageSummary {
    type Error = ContractError;
    fn try_from(value: StateSummary<'a>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value).map_err(|_| ContractError::InvalidState)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Verification {
    public_key: ed25519_dalek::PublicKey,
}

impl Verification {
    fn verify(&self, msg: &Post) -> bool {
        if let Some(sig) = msg.signature {
            self.public_key
                .verify(&serde_json::to_vec(msg).unwrap(), &sig)
                .is_ok()
        } else {
            false
        }
    }
}

impl<'a> TryFrom<Parameters<'a>> for Verification {
    type Error = ContractError;
    fn try_from(value: Parameters<'a>) -> Result<Self, Self::Error> {
        serde_json::from_slice(value.as_ref()).map_err(|_| ContractError::InvalidState)
    }
}

#[contract]
impl ContractInterface for PostsFeed {
    fn validate_state(_parameters: Parameters<'static>, state: State<'static>) -> bool {
        PostsFeed::try_from(state).is_ok()
    }

    fn validate_delta(_parameters: Parameters<'static>, delta: StateDelta<'static>) -> bool {
        serde_json::from_slice::<Vec<Post>>(&delta).is_ok()
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        delta: StateDelta<'static>,
    ) -> Result<UpdateModification, ContractError> {
        let mut feed = PostsFeed::try_from(state)?;
        let verifier = Verification::try_from(parameters).ok();
        feed.messages.sort_by_cached_key(|m| m.hash());
        let mut incoming =
            serde_json::from_slice::<Vec<Post>>(&delta).map_err(|_| ContractError::InvalidDelta)?;
        incoming.sort_by_cached_key(|m| m.hash());
        for m in incoming {
            if feed
                .messages
                .binary_search_by_key(&m.hash(), |o| o.hash())
                .is_err()
            {
                if m.mod_msg {
                    if let Some(verifier) = &verifier {
                        if !verifier.verify(&m) {
                            continue;
                        }
                        feed.messages.push(m);
                    }
                } else {
                    feed.messages.push(m);
                }
            }
        }

        let feed_bytes: Vec<u8> =
            serde_json::to_vec(&feed).map_err(|err| ContractError::Other(err.into()))?;
        Ok(UpdateModification::ValidUpdate(State::from(feed_bytes)))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> StateSummary<'static> {
        let mut feed = PostsFeed::try_from(state).unwrap();
        let only_messages = FeedSummary::from(&mut feed);
        StateSummary::from(serde_json::to_vec(&only_messages).expect("serialization failed"))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> StateDelta<'static> {
        let feed = PostsFeed::try_from(state).unwrap();
        let mut summary = match serde_json::from_slice::<FeedSummary>(&summary) {
            Ok(summary) => summary,
            Err(_) => {
                // empty summary
                FeedSummary { summaries: vec![] }
            }
        };
        summary.summaries.sort();
        let mut final_messages = vec![];
        for msg in feed.messages {
            let mut hasher = Blake2b512::new();
            hasher.update(msg.author.as_bytes());
            hasher.update(msg.title.as_bytes());
            hasher.update(msg.content.as_bytes());
            let hash_val = hasher.finalize();
            if summary
                .summaries
                .binary_search_by(|m| m.0.as_ref().cmp(&hash_val[..]))
                .is_err()
            {
                final_messages.push(msg);
            }
        }
        StateDelta::from(serde_json::to_vec(&final_messages).unwrap())
    }
}

#[cfg(test)]
mod test {
    use byteorder::{BigEndian, WriteBytesExt};
    use serde_json::Value;

    use super::*;

    fn get_test_state(mut data: Vec<u8>) -> Vec<u8> {
        let mut state: Vec<u8> = vec![];
        let metadata: &[u8] = &[];
        state.write_u64::<BigEndian>(metadata.len() as u64).unwrap();
        state.write_u64::<BigEndian>(data.len() as u64).unwrap();
        state.append(&mut data);
        state
    }

    #[test]
    fn conversions() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "messages": [
                {
                    "author": "IDG",
                    "date": "2022-05-10T00:00:00Z",
                    "title": "Lore ipsum",
                    "content": "..."
                }
            ]
        }"#;
        let _feed = PostsFeed::try_from(State::from(json.as_bytes()))?;
        Ok(())
    }

    #[test]
    fn validate_state() {
        let json_bytes = r#"{
            "messages": [
                {
                    "author": "IDG",
                    "date": "2022-05-10T00:00:00Z",
                    "title": "Lore ipsum",
                    "content": "..."
                }
            ]
        }"#
        .as_bytes()
        .to_vec();

        let state: Vec<u8> = get_test_state(json_bytes);
        let valid = PostsFeed::validate_state([].as_ref().into(), State::from(state));
        assert!(valid);
    }

    #[test]
    fn validate_delta() {
        let json = r#"[
            {
                "author": "IDG",
                "date": "2022-05-10T00:00:00Z",
                "title": "Lore ipsum",
                "content": "..."
            }
        ]"#;
        let valid = PostsFeed::validate_delta(
            [].as_ref().into(),
            StateDelta::from(json.as_bytes().to_vec()),
        );
        assert!(valid);
    }
    #[test]
    fn update_state() {
        let state_bytes = r#"{"messages":[{"author":"IDG","content":"...",
        "date":"2022-05-10T00:00:00Z","title":"Lore ipsum"}]}"#
            .as_bytes()
            .to_vec();
        let state: Vec<u8> = get_test_state(state_bytes);

        let _delta =
            r#"[{"author":"IDG","content":"...","date":"2022-06-15T00:00:00Z","title":"New msg"}]"#;

        let delta = StateDelta::from(vec![
            123u8, 10, 9, 9, 9, 34, 97, 117, 116, 104, 111, 114, 34, 58, 32, 34, 73, 68, 71, 34,
            44, 10, 9, 9, 9, 34, 100, 97, 116, 101, 34, 58, 32, 34, 50, 48, 50, 50, 45, 48, 53, 45,
            49, 48, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 34, 44, 10, 9, 9, 9, 34, 116, 105, 116,
            108, 101, 34, 58, 32, 34, 76, 111, 114, 101, 32, 105, 112, 115, 117, 109, 34, 44, 10,
            9, 9, 9, 34, 99, 111, 110, 116, 101, 110, 116, 34, 58, 32, 34, 76, 111, 114, 101, 109,
            32, 105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97,
            109, 101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97,
            100, 105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 44, 32, 115, 101,
            100, 32, 100, 111, 32, 101, 105, 117, 115, 109, 111, 100, 32, 116, 101, 109, 112, 111,
            114, 32, 105, 110, 99, 105, 100, 105, 100, 117, 110, 116, 32, 117, 116, 32, 108, 97,
            98, 111, 114, 101, 32, 101, 116, 32, 100, 111, 108, 111, 114, 101, 32, 109, 97, 103,
            110, 97, 32, 97, 108, 105, 113, 117, 97, 46, 34, 10, 9, 9, 125,
        ]);

        let new_state = PostsFeed::update_state([].as_ref().into(), state.into(), delta)
            .unwrap()
            .unwrap_valid();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(new_state.as_ref()).unwrap(),
            serde_json::json!({
                "messages": [
                    {
                        "author": "IDG",
                        "date": "2022-05-10T00:00:00Z",
                        "mod_msg": false,
                        "signature": Value::Null,
                        "title": "Lore ipsum",
                        "content": "..."
                    },
                    {
                        "author": "IDG",
                        "date": "2022-06-15T00:00:00Z",
                        "mod_msg": false,
                        "signature": Value::Null,
                        "title": "New msg",
                        "content": "..."
                    }
                ]
            })
        );
    }

    #[test]
    fn summarize_state() {
        let state_bytes = r#"{
            "messages": [
                {
                    "author": "IDG",
                    "date": "2022-05-10T00:00:00Z",
                    "title": "Lore ipsum",
                    "content": "..."
                }
            ]
        }"#
        .as_bytes()
        .to_vec();

        let state: Vec<u8> = get_test_state(state_bytes);
        let summary = PostsFeed::summarize_state([].as_ref().into(), State::from(state));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(summary.as_ref()).unwrap(),
            serde_json::json!([{
                "author": "IDG",
                "date": "2022-05-10T00:00:00Z",
                "title": "Lore ipsum",
                "content": "..."
            }])
        );
    }

    #[test]
    fn get_state_delta() {
        let state_bytes = r#"{
            "messages": [
                {
                    "author": "IDG",
                    "date": "2022-05-11T00:00:00Z",
                    "title": "Lore ipsum",
                    "content": "..."
                },
                {
                    "author": "IDG",
                    "date": "2022-04-10T00:00:00Z",
                    "title": "Lore ipsum",
                    "content": "..."
                }
            ]
        }"#
        .as_bytes()
        .to_vec();
        let state: Vec<u8> = get_test_state(state_bytes);
        let summary = serde_json::json!([{
                "author": "IDG",
                "date": "2022-04-10T00:00:00Z",
                "title": "Lore ipsum",
                "content": "..."
        }]);

        let delta = PostsFeed::get_state_delta(
            [].as_ref().into(),
            State::from(state),
            serde_json::to_vec(&summary).unwrap().into(),
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(delta.as_ref()).unwrap(),
            serde_json::json!([{
                "author": "IDG",
                "date": "2022-05-11T00:00:00Z",
                "title": "Lore ipsum",
                "content": "..."
            }])
        );
    }
}
