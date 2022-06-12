use chrono::{DateTime, Utc};
use locutus_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct MessageFeed {
    messages: Vec<Message>,
}

#[derive(Serialize, Deserialize)]
struct Message {
    author: String,
    date: DateTime<Utc>,
    title: String,
    content: String,
}

impl<'a> TryFrom<State<'a>> for MessageFeed {
    type Error = ContractError;

    fn try_from(value: State<'a>) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value).map_err(|_| ContractError::InvalidState)
    }
}

// #[derive(Serialize, Deserialize)]
// struct FeedSummary {
//     messages: Vec<MessageSummary>,
// }

// #[derive(Serialize, Deserialize)]
// struct MessageSummary {
//     author: String,
//     date: DateTime<Utc>,
//     title: String,
// }

// impl<'a> TryFrom<StateSummary<'a>> for MessageSummary {
//     type Error = ContractError;
//     fn try_from(value: StateSummary<'a>) -> Result<Self, Self::Error> {
//         serde_json::from_slice(&value).map_err(|_| ContractError::InvalidState)
//     }
// }

#[contract]
impl ContractInterface for MessageFeed {
    fn validate_state(_parameters: Parameters<'static>, state: State<'static>) -> bool {
        MessageFeed::try_from(state).is_ok()
    }

    fn validate_delta(_parameters: Parameters<'static>, delta: StateDelta<'static>) -> bool {
        serde_json::from_slice::<Vec<Message>>(&delta).is_ok()
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        delta: StateDelta<'static>,
    ) -> Result<UpdateModification, ContractError> {
        let mut feed = MessageFeed::try_from(state)?;
        feed.messages.sort_by_key(|m| m.date);
        let mut incoming = serde_json::from_slice::<Vec<Message>>(&delta)
            .map_err(|_| ContractError::InvalidDelta)?;
        incoming.sort_by_key(|m| m.date);
        for m in incoming {
            if feed
                .messages
                .binary_search_by_key(&m.date, |o| o.date)
                .is_err()
            {
                feed.messages.push(m);
            }
        }
        Ok(UpdateModification::ValidUpdate(State::from(
            serde_json::to_string(&feed)
                .map_err(|err| ContractError::Other(err.into()))?
                .into_bytes(),
        )))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> StateSummary<'static> {
        let feed = MessageFeed::try_from(state).unwrap();
        let only_messages = serde_json::to_string(&feed.messages)
            .map_err(|err| ContractError::Other(err.into()))
            .unwrap();
        StateSummary::from(only_messages.into_bytes())
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> StateDelta<'static> {
        let mut feed = MessageFeed::try_from(state).unwrap();
        feed.messages.sort_by_key(|m| m.date);
        let mut current = serde_json::from_slice::<Vec<Message>>(&summary)
            .map_err(|_| ContractError::InvalidDelta)
            .unwrap();
        current.sort_by_key(|m| m.date);
        let mut delta = vec![];
        for m in feed.messages {
            if current.binary_search_by_key(&m.date, |o| o.date).is_err() {
                delta.push(m);
            }
        }
        let serialized = serde_json::to_string(&delta)
            .map_err(|err| ContractError::Other(err.into()))
            .unwrap();
        StateDelta::from(serialized.into_bytes())
    }

    fn update_state_from_summary(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<UpdateModification, ContractError> {
        let mut feed = MessageFeed::try_from(state).unwrap();
        let mut summary = serde_json::from_slice::<Vec<Message>>(&summary)
            .map_err(|_| ContractError::InvalidDelta)
            .unwrap();
        feed.messages.append(&mut summary);
        Ok(UpdateModification::ValidUpdate(
            serde_json::to_string(&feed).unwrap().into_bytes().into(),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
        let _feed = MessageFeed::try_from(State::from(json.as_bytes()))?;
        Ok(())
    }

    #[test]
    fn validate_state() {
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
        let valid =
            MessageFeed::validate_state([].as_ref().into(), State::from(json.as_bytes().to_vec()));
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
        let valid = MessageFeed::validate_delta(
            [].as_ref().into(),
            StateDelta::from(json.as_bytes().to_vec()),
        );
        assert!(valid);
    }

    #[test]
    fn summarize_state() {
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
        let summary =
            MessageFeed::summarize_state([].as_ref().into(), State::from(json.as_bytes().to_vec()));
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
        let json = r#"{
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
        }"#;
        let summary = serde_json::json!([{
                "author": "IDG",
                "date": "2022-04-10T00:00:00Z",
                "title": "Lore ipsum",
                "content": "..."
        }]);
        let delta = MessageFeed::get_state_delta(
            [].as_ref().into(),
            State::from(json.as_bytes().to_vec()),
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

    #[test]
    fn update_state_from_summary() {
        let state = r#"{
            "messages": [
                {
                    "author": "IDG",
                    "date": "2022-05-11T00:00:00Z",
                    "title": "Lore ipsum",
                    "content": "..."
                }    
            ]
        }"#;
        let summary = serde_json::json!([{
                "author": "IDG",
                "date": "2022-04-10T00:00:00Z",
                "title": "Lore ipsum",
                "content": "..."
        }]);
        let delta = MessageFeed::update_state_from_summary(
            [].as_ref().into(),
            State::from(state.as_bytes().to_vec()),
            serde_json::to_vec(&summary).unwrap().into(),
        )
        .unwrap()
        .unwrap_valid();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(delta.as_ref()).unwrap(),
            serde_json::json!({
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
            })
        );
    }
}
