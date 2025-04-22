use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

// Constants to simplify the test
const TEST_DATA: [u8; 3] = [1, 2, 3];
const TEST_RESPONSE: [u8; 3] = [4, 5, 6];

#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    TestRequest(String),
}

#[derive(Debug, Serialize, Deserialize)]
enum OutboundAppMessage {
    TestResponse(String, Vec<u8>),
}

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        _params: Parameters<'static>,
        _attested: Option<&'static [u8]>,
        messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match messages {
            InboundDelegateMsg::ApplicationMessage(incoming_app) => {
                // Deserialize the incoming message
                let message: InboundAppMessage = bincode::deserialize(incoming_app.payload.as_slice())
                    .map_err(|err| DelegateError::Other(format!("Error deserializing message: {err}")))?;

                match message {
                    InboundAppMessage::TestRequest(request_data) => {
                        // Prepare the response
                        let response_msg_content = OutboundAppMessage::TestResponse(
                            format!("Processed: {}", request_data),
                            TEST_RESPONSE.to_vec(),
                        );
                        
                        // Serialize the response
                        let payload: Vec<u8> = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("Error serializing response: {err}")))?;
                        
                        // Create the response message for the application
                        let response_app_msg = ApplicationMessage::new(incoming_app.app, payload)
                            .processed(true)
                            .with_context(incoming_app.context);
                        
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response_app_msg)])
                    }
                }
            },
            _ => Err(DelegateError::Other("Unsupported message type".to_string())),
        }
    }
}

#[test]
fn test_delegate() -> Result<(), Box<dyn std::error::Error>> {
    let contract = WrappedContract::new(
        std::sync::Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let app_id = ContractInstanceId::try_from(contract.key.to_string()).unwrap();
    
    let request_data = "test-data".to_string();
    let payload: Vec<u8> = bincode::serialize(&InboundAppMessage::TestRequest(request_data.clone())).unwrap();
    let test_request_msg = ApplicationMessage::new(app_id, payload).processed(false);
    
    let inbound = InboundDelegateMsg::ApplicationMessage(test_request_msg);
    let output = Delegate::process(Parameters::from(vec![]), None, inbound)?;
    
    assert_eq!(output.len(), 1, "There should be exactly one output message");
    
    let app_response = match output.first().unwrap() {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        _ => panic!("Expected an ApplicationMessage"),
    };
    
    assert!(app_response.processed, "The message should be marked as processed");
    
    let app_response_payload: OutboundAppMessage = bincode::deserialize(app_response.payload.as_ref())?;

    match app_response_payload {
        OutboundAppMessage::TestResponse(response_text, response_data) => {
            assert_eq!(response_text, format!("Processed: {}", request_data));
            assert_eq!(response_data, TEST_RESPONSE);
        }
    }
    
    Ok(())
} 