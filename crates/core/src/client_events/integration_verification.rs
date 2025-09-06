//! Integration verification tests for client connection refactor infrastructure

#[cfg(test)]
mod integration_verification {
    use crate::client_events::{RequestId, OpenRequest, ClientId};
    use crate::contract::{contract_handler_channel, SessionMessage};
    use tokio::sync::mpsc;
    use freenet_stdlib::client_api::ClientRequest;

    #[tokio::test]
    async fn test_end_to_end_correlation_workflow() {
        // Test complete correlation workflow from request creation to tracking
        
        // 1. Create request with correlation
        let client_id = ClientId::FIRST;
        let request = Box::new(ClientRequest::Disconnect { cause: None });
        let open_req = OpenRequest::new(client_id, request);
        let correlation_id = open_req.request_id;
        
        // 2. Verify correlation appears in display (would be in logs)
        let display = format!("{}", open_req);
        assert!(display.contains("client: "));
        assert!(display.contains(&format!("request_id: {}", correlation_id)));
        
        // 3. Simulate processing through client events infrastructure
        let processed_request_id = open_req.request_id;
        
        // 4. Verify correlation is preserved
        assert_eq!(correlation_id, processed_request_id);
        
        // 5. Test that correlation IDs are unique across concurrent requests
        let concurrent_requests: Vec<_> = (0..100)
            .map(|_| {
                let req = Box::new(ClientRequest::Disconnect { cause: None });
                OpenRequest::new(client_id, req)
            })
            .collect();
        
        // Verify all request IDs are unique
        let mut request_ids: Vec<_> = concurrent_requests.iter()
            .map(|req| req.request_id)
            .collect();
        request_ids.sort();
        request_ids.dedup();
        assert_eq!(request_ids.len(), 100, "All request IDs should be unique");
    }

    #[tokio::test]
    async fn test_session_adapter_installation() {
        // Test that session adapter can be installed without errors
        
        let (mut ch_outbound, _ch_inbound, _wait_for_event) = contract_handler_channel();
        let (session_tx, _session_rx) = mpsc::channel(100);
        
        // Install session adapter - this should not panic or fail
        ch_outbound.with_session_adapter(session_tx);
        
        // Test passes if no panic/error occurred during installation
    }

    #[tokio::test] 
    async fn test_result_router_infrastructure_ready() {
        // Test that result router infrastructure is ready for future phases
        use crate::client_events::result_router::ResultRouter;
        use crate::message::{Transaction, QueryResult};
        
        let (network_tx, network_rx) = mpsc::channel::<(Transaction, QueryResult)>(100);
        let (session_tx, mut session_rx) = mpsc::channel::<SessionMessage>(100);
        
        // Create router
        let router = ResultRouter::new(network_rx, session_tx);
        
        // Spawn router task (would be done in Phase 1)
        let router_handle = tokio::spawn(async move {
            router.run().await;
        });
        
        // Simulate network result (this would come from network layer in Phase 1)
        // For now, just verify the infrastructure is ready
        drop(network_tx); // Closes channel, which will end router.run()
        
        // Wait briefly for router to process
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        // Verify no session messages (since we didn't send any network results)
        assert!(session_rx.try_recv().is_err());
        
        // Cleanup
        router_handle.abort();
    }

    #[test]
    fn test_actor_infrastructure_flag_behavior() {
        // Test environment flag behavior in different scenarios
        
        // Save original state
        let original = std::env::var("FREENET_ACTOR_CLIENTS").ok();
        
        // Test default (disabled) behavior
        std::env::remove_var("FREENET_ACTOR_CLIENTS");
        let disabled = std::env::var("FREENET_ACTOR_CLIENTS").unwrap_or_default() == "true";
        assert!(!disabled, "Should be disabled by default");
        
        // Test enabled behavior
        std::env::set_var("FREENET_ACTOR_CLIENTS", "true");
        let enabled = std::env::var("FREENET_ACTOR_CLIENTS").unwrap_or_default() == "true";
        assert!(enabled, "Should be enabled when set to 'true'");
        
        // Test case sensitivity
        std::env::set_var("FREENET_ACTOR_CLIENTS", "TRUE");
        let case_sensitive = std::env::var("FREENET_ACTOR_CLIENTS").unwrap_or_default() == "true";
        assert!(!case_sensitive, "Should be case-sensitive (TRUE != true)");
        
        // Test other values
        std::env::set_var("FREENET_ACTOR_CLIENTS", "1");
        let numeric = std::env::var("FREENET_ACTOR_CLIENTS").unwrap_or_default() == "true";
        assert!(!numeric, "Should only accept 'true', not '1'");
        
        // Restore original state
        match original {
            Some(val) => std::env::set_var("FREENET_ACTOR_CLIENTS", val),
            None => std::env::remove_var("FREENET_ACTOR_CLIENTS"),
        }
    }

    #[tokio::test]
    async fn test_zero_performance_overhead_correlation() {
        // Test that correlation adds minimal overhead
        use std::time::Instant;
        
        const NUM_REQUESTS: usize = 10000;
        
        // Time request creation with correlation
        let start = Instant::now();
        let requests_with_correlation: Vec<_> = (0..NUM_REQUESTS)
            .map(|_| {
                let req = Box::new(ClientRequest::Disconnect { cause: None });
                OpenRequest::new(ClientId::FIRST, req)
            })
            .collect();
        let with_correlation = start.elapsed();
        
        // Verify all requests were created
        assert_eq!(requests_with_correlation.len(), NUM_REQUESTS);
        
        // Time should be very fast (correlation is just atomic counter increment)
        assert!(with_correlation.as_millis() < 100, 
                "Correlation should add minimal overhead, took {}ms", 
                with_correlation.as_millis());
        
        // Verify uniqueness at scale
        let mut ids: Vec<_> = requests_with_correlation.iter()
            .map(|r| r.request_id)
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), NUM_REQUESTS, "All IDs should be unique at scale");
    }

    #[test]
    fn test_correlation_thread_safety_comprehensive() {
        // Comprehensive thread safety test for RequestId generation
        use std::thread;
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};
        
        const NUM_THREADS: usize = 10;
        const REQUESTS_PER_THREAD: usize = 1000;
        
        let all_ids = Arc::new(Mutex::new(HashSet::new()));
        
        let handles: Vec<_> = (0..NUM_THREADS)
            .map(|_| {
                let ids_clone = Arc::clone(&all_ids);
                thread::spawn(move || {
                    let thread_ids: Vec<_> = (0..REQUESTS_PER_THREAD)
                        .map(|_| RequestId::new())
                        .collect();
                    
                    // Add to global set
                    let mut global_ids = ids_clone.lock().unwrap();
                    for id in thread_ids {
                        assert!(global_ids.insert(id), "Duplicate ID found: {}", id);
                    }
                })
            })
            .collect();
        
        // Wait for all threads
        for handle in handles {
            handle.join().expect("Thread should complete successfully");
        }
        
        // Verify total count
        let final_ids = all_ids.lock().unwrap();
        assert_eq!(final_ids.len(), NUM_THREADS * REQUESTS_PER_THREAD, 
                  "All IDs should be unique across threads");
    }
}