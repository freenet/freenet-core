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
        serde_json::from_slice(value.as_ref()).map_err(|_| ContractError::InvalidState)
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
            serde_json::to_vec(&feed).map_err(|err| ContractError::Other(err.into()))?,
        )))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> StateSummary<'static> {
        let feed = MessageFeed::try_from(state).unwrap();
        let only_messages = serde_json::to_vec(&feed.messages)
            .map_err(|err| ContractError::Other(err.into()))
            .unwrap();
        StateSummary::from(only_messages)
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
        let serialized = serde_json::to_vec(&delta)
            .map_err(|err| ContractError::Other(err.into()))
            .unwrap();
        StateDelta::from(serialized)
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
            serde_json::to_vec(&feed).unwrap().into(),
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
    fn update_state() {
        /*

                [2022-06-15T17:28:27Z INFO  locutus_dev::local_node] s: {"messages":[{"author":"IDG","content":"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.","date":"2022-05-10T00:00:00Z","title":"Lore ipsum"}]}
        [2022-06-15T17:28:27Z INFO  locutus_dev::local_node] d: [{"author":"IDG","content":"...","date":"2022-05-10T00:00:00Z","title":"Lore ipsum"}]
                */
        let state = r#"{"messages":[{"author":"IDG","content":"...","date":"2022-05-10T00:00:00Z","title":"Lore ipsum"}]}"#;
        let delta =
            r#"[{"author":"IDG","content":"...","date":"2022-06-15T00:00:00Z","title":"New msg"}]"#;
        let new_state = MessageFeed::update_state(
            [].as_ref().into(),
            state.as_bytes().to_vec().into(),
            delta.as_bytes().to_vec().into(),
        )
        .unwrap()
        .unwrap_valid();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(new_state.as_ref()).unwrap(),
            serde_json::json!({
                "messages": [
                    {
                        "author": "IDG",
                        "date": "2022-05-10T00:00:00Z",
                        "title": "Lore ipsum",
                        "content": "..."
                    },
                    {
                        "author": "IDG",
                        "date": "2022-06-15T00:00:00Z",
                        "title": "New msg",
                        "content": "..."
                    }
                ]
            })
        );
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

    #[test]
    fn dummy_test() {
        let arr = &[
            123, 34, 109, 101, 115, 115, 97, 103, 101, 115, 34, 58, 91, 123, 34, 97, 117, 116, 104,
            111, 114, 34, 58, 34, 73, 68, 71, 34, 44, 34, 100, 97, 116, 101, 34, 58, 34, 50, 48,
            50, 50, 45, 48, 53, 45, 49, 48, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 34, 44, 34,
            116, 105, 116, 108, 101, 34, 58, 34, 76, 111, 114, 101, 32, 105, 112, 115, 117, 109,
            34, 44, 34, 99, 111, 110, 116, 101, 110, 116, 34, 58, 34, 76, 111, 114, 101, 109, 32,
            105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109,
            101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97, 100,
            105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 44, 32, 115, 101, 100,
            32, 100, 111, 32, 101, 105, 117, 115, 109, 111, 100, 32, 116, 101, 109, 112, 111, 114,
            32, 105, 110, 99, 105, 100, 105, 100, 117, 110, 116, 32, 117, 116, 32, 108, 97, 98,
            111, 114, 101, 32, 101, 116, 32, 100, 111, 108, 111, 114, 101, 32, 109, 97, 103, 110,
            97, 32, 97, 108, 105, 113, 117, 97, 46, 34, 125, 44, 123, 34, 97, 117, 116, 104, 111,
            114, 34, 58, 34, 73, 68, 71, 34, 44, 34, 100, 97, 116, 101, 34, 58, 34, 50, 48, 50, 50,
            45, 48, 54, 45, 49, 53, 84, 48, 48, 58, 48, 48, 58, 48, 48, 90, 34, 44, 34, 116, 105,
            116, 108, 101, 34, 58, 34, 78, 101, 119, 32, 109, 115, 103, 34, 44, 34, 99, 111, 110,
            116, 101, 110, 116, 34, 58, 34, 46, 46, 46, 34, 125, 93, 125,
        ];
        let str = String::from_utf8(arr.to_vec()).unwrap();
        println!("str: {str:?}");
        let json_str = serde_json::from_slice::<serde_json::Value>(arr).unwrap();
        println!("json: {json_str:?}");

        let summary = MessageFeed::summarize_state([].as_ref().into(), arr.to_vec().into());
        let json_str = String::from_utf8(summary.as_ref().to_vec()).unwrap();
        println!("{json_str}");

        // let state = r#"[
        //     {
        //         "author": "IDG",
        //         "date": "2022-05-18T00:00:00Z",
        //         "title": "Lore ipsum",
        //         "content": "..."
        //     }
        // ]"#;
        // MessageFeed::summarize_state([].as_ref().into(), state);
    }
}
