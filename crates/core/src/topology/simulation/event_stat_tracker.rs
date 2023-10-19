use std::collections::LinkedList;

const MAX_EVENTS_TO_TRACK: usize = 100;

#[derive(Debug)]
pub(crate) struct EventStatTracker {
    recent_event_times: LinkedList<u64>,
}

impl EventStatTracker {
    pub(crate) fn new() -> Self {
        Self {
            recent_event_times: LinkedList::new(),
        }
    }

    pub(crate) fn get_event_times(&self) -> &LinkedList<u64> {
        &self.recent_event_times
    }

    pub(crate) fn add_event(&mut self, time: u64) {
        self.recent_event_times.push_back(time);
        // if there are recent_event_times remove the oldest event times if the exceed MAX_EVENTS_TO_TRACK
        while self.recent_event_times.len() > MAX_EVENTS_TO_TRACK {
            self.recent_event_times.pop_front();
        }
    }
}