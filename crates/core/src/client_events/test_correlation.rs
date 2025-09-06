//! Request correlation testing

#[cfg(test)]
mod tests {
    use crate::client_events::{OpenRequest, RequestId};
    use freenet_stdlib::client_api::ClientRequest;
    use crate::client_events::ClientId;

    #[test]
    fn test_request_id_generation_and_uniqueness() {
        let id1 = RequestId::new();
        let id2 = RequestId::new();
        
        // Request IDs should be unique
        assert_ne!(id1, id2);
        
        // Self-equality should work
        assert_eq!(id1, id1);
        
        // Display should work
        let id1_str = format!("{}", id1);
        assert!(id1_str.starts_with("req-"));
    }

    #[test]
    fn test_open_request_with_correlation() {
        let request_id = RequestId::new();
        let client_id = ClientId::FIRST;
        
        let req = OpenRequest {
            client_id,
            request_id,
            request: Box::new(ClientRequest::Disconnect { cause: None }),
            notification_channel: None,
            token: None,
            attested_contract: None,
        };
        
        assert_eq!(req.request_id, request_id);
        assert_eq!(req.client_id, client_id);
    }

    #[test]
    fn test_request_id_display_format() {
        let request_id = RequestId::new();
        let display_str = format!("{}", request_id);
        
        // Should start with "req-"
        assert!(display_str.starts_with("req-"));
        
        // Should have more than just "req-"
        assert!(display_str.len() > 4);
        
        // Should be consistent
        assert_eq!(display_str, format!("{}", request_id));
    }

    #[test]
    fn test_open_request_new_method_generates_request_id() {
        let client_id = ClientId::FIRST;
        let request = Box::new(ClientRequest::Disconnect { cause: None });
        
        let open_req = OpenRequest::new(client_id, request);
        
        // Should have generated a request_id
        let id_str = format!("{}", open_req.request_id);
        assert!(id_str.starts_with("req-"));
        
        // Should preserve client_id
        assert_eq!(open_req.client_id, client_id);
    }

    #[test]
    fn test_open_request_display_includes_correlation() {
        let open_req = OpenRequest::new(
            ClientId::FIRST,
            Box::new(ClientRequest::Disconnect { cause: None })
        );
        
        let display_str = format!("{}", open_req);
        
        // Should include client ID
        assert!(display_str.contains("client: "));
        
        // Should include request ID
        assert!(display_str.contains("request_id: "));
        
        // Should include the request
        assert!(display_str.contains("req: "));
    }
}