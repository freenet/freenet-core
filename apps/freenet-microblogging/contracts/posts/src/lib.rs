use freenet_stdlib::prelude::{
    blake3::{traits::digest::Digest, Hasher as Blake3},
    *,
};
use rsa::{self, pkcs1v15::VerifyingKey, sha2::Sha256};
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

type Signature = Box<[u8]>;

#[derive(Serialize, Deserialize)]
pub struct Post {
    pub author: String,
    // date: DateTime<Utc>,
    pub title: String,
    pub content: String,
    #[serde(default = "Post::modded")]
    pub mod_msg: bool,
    pub signature: Option<Signature>,
}

impl Post {
    pub fn hash(&self) -> [u8; 32] {
        let mut hasher = Blake3::new();
        hasher.update(self.author.as_bytes());
        hasher.update(self.title.as_bytes());
        hasher.update(self.content.as_bytes());
        let hash_val = hasher.finalize();
        let mut key = [0; 32];
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
struct MessageSummary(#[serde_as(as = "[_; 32]")] [u8; 32]);

impl<'a> TryFrom<StateSummary<'a>> for MessageSummary {
    type Error = ContractError;
    fn try_from(value: StateSummary<'a>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value).map_err(|_| ContractError::InvalidState)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Verification {
    public_key: rsa::RsaPublicKey,
}

impl Verification {
    fn verify(&self, msg: &Post) -> bool {
        if let Some(sig) = &msg.signature {
            use rsa::signature::Verifier;
            let verifying_key = VerifyingKey::<Sha256>::new(self.public_key.clone());
            verifying_key
                .verify(
                    &serde_json::to_vec(msg).unwrap(),
                    &rsa::pkcs1v15::Signature::try_from(&**sig).unwrap(),
                )
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
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts,
    ) -> Result<ValidateResult, ContractError> {
        PostsFeed::try_from(state).map(|_| ValidateResult::Valid)
    }

    fn validate_delta(
        _parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        serde_json::from_slice::<Vec<Post>>(&delta).map_err(|_| ContractError::InvalidDelta)?;
        Ok(true)
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        delta: Vec<UpdateData>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let mut feed = PostsFeed::try_from(state)?;
        let verifier = Verification::try_from(parameters).ok();
        feed.messages.sort_by_cached_key(|m| m.hash());
        let mut incoming = serde_json::from_slice::<Vec<Post>>(delta[0].unwrap_delta())
            .map_err(|_| ContractError::InvalidDelta)?;
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
            serde_json::to_vec(&feed).map_err(|err| ContractError::Other(format!("{err}")))?;
        Ok(UpdateModification::valid(State::from(feed_bytes)))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let mut feed = PostsFeed::try_from(state).unwrap();
        let only_messages = FeedSummary::from(&mut feed);
        Ok(StateSummary::from(
            serde_json::to_vec(&only_messages)
                .map_err(|err| ContractError::Other(format!("{err}")))?,
        ))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
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
            let mut hasher = Blake3::new();
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
        Ok(StateDelta::from(
            serde_json::to_vec(&final_messages)
                .map_err(|err| ContractError::Other(format!("{err}")))?,
        ))
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
    fn validate_state() -> Result<(), Box<dyn std::error::Error>> {
        let state = r#"{
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

        let valid = PostsFeed::validate_state(
            [].as_ref().into(),
            State::from(state),
            RelatedContracts::new(),
        )?;
        assert!(matches!(valid, ValidateResult::Valid));
        Ok(())
    }

    #[test]
    fn validate_delta() -> Result<(), Box<dyn std::error::Error>> {
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
        )?;
        assert!(valid);
        Ok(())
    }

    #[test]
    fn update_state() -> Result<(), Box<dyn std::error::Error>> {
        let state = r#"{"messages":[{"author":"IDG","content":"...",
        "date":"2022-05-10T00:00:00Z","title":"Lore ipsum"}]}"#
            .as_bytes()
            .to_vec();

        let _delta =
            r#"[{"author":"IDG","content":"...","date":"2022-06-15T00:00:00Z","title":"New msg"}]"#;

        let delta = StateDelta::from(vec![
            91, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 123, 10,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
            34, 97, 117, 116, 104, 111, 114, 34, 58, 34, 72, 83, 34, 44, 10, 32, 32, 32, 32, 32,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 34, 100, 97, 116,
            101, 34, 58, 34, 50, 48, 50, 50, 45, 48, 54, 45, 49, 53, 84, 48, 48, 58, 48, 48, 58,
            48, 48, 90, 34, 44, 10, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
            32, 32, 32, 32, 32, 32, 34, 116, 105, 116, 108, 101, 34, 58, 34, 78, 101, 119, 32, 109,
            115, 103, 34, 44, 10, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
            32, 32, 32, 32, 32, 32, 34, 99, 111, 110, 116, 101, 110, 116, 34, 58, 34, 76, 111, 114,
            101, 32, 105, 112, 115, 117, 109, 32, 46, 46, 46, 34, 10, 32, 32, 32, 32, 32, 32, 32,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 125, 10, 32, 32, 32, 32, 32, 32, 32, 32,
            32, 32, 93,
        ]);

        let new_state = PostsFeed::update_state(
            [].as_ref().into(),
            state.into(),
            vec![UpdateData::Delta(delta)],
        )
        .unwrap()
        .unwrap_valid();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(new_state.as_ref()).unwrap(),
            serde_json::json!({
                "messages": [
                    {
                        "author": "IDG",
                        "mod_msg": false,
                        "signature": null,
                        "title": "Lore ipsum",
                        "content": "..."
                    },
                    {
                        "author": "HS",
                        "mod_msg": false,
                        "signature": null,
                        "title": "New msg",
                        "content": "Lore ipsum ..."
                    }
                ]
            })
        );
        Ok(())
    }

    #[test]
    fn summarize_state() -> Result<(), Box<dyn std::error::Error>> {
        let state = r#"{
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

        let posts_feed = PostsFeed::summarize_state([].as_ref().into(), State::from(state))?;
        let feed_summary = serde_json::from_slice::<FeedSummary>(posts_feed.as_ref()).unwrap();
        assert_eq!(feed_summary.summaries.len(), 1);
        Ok(())
    }

    #[test]
    fn get_state_delta() -> Result<(), Box<dyn std::error::Error>> {
        let state = r#"{
            "messages": [
                {
                    "author": "IDG",
                    "date": "2022-05-11T00:00:00Z",
                    "title": "Lore ipsum",
                    "content": "..."
                },
                {
                    "author": "HS",
                    "date": "2022-04-10T00:00:00Z",
                    "title": "Lore ipsum",
                    "content": "..."
                }
            ]
        }"#
        .as_bytes()
        .to_vec();

        let summary_state = r#"{
            "messages": [
                {
                    "author": "IDG",
                    "date": "2022-05-11T00:00:00Z",
                    "title": "Lore ipsum",
                    "content": "..."
                }
            ]
        }"#
        .as_bytes()
        .to_vec();

        let mut summary_posts = PostsFeed::try_from(State::from(summary_state))?;
        let summary = FeedSummary::from(&mut summary_posts);

        let delta = PostsFeed::get_state_delta(
            [].as_ref().into(),
            State::from(state),
            serde_json::to_vec(&summary).unwrap().into(),
        )?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(delta.as_ref()).unwrap(),
            serde_json::json!([{
                "author": "HS",
                "mod_msg": false,
                "signature": null,
                "title": "Lore ipsum",
                "content": "...",
            }])
        );
        Ok(())
    }
}
