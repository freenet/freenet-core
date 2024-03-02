use crate::transport::peer_connection::outbound_stream::SerializedStream;
use std::collections::BTreeMap;

pub(super) struct InboundStream {
    total_length_bytes: u64,
    /// Fragment numbers are 1-indexed
    last_contiguous_fragment_idx: u32,
    non_contiguous_fragments: BTreeMap<u32, Vec<u8>>,
    payload: Vec<u8>,
}

impl InboundStream {
    pub fn new(total_length_bytes: u64) -> Self {
        Self {
            total_length_bytes,
            last_contiguous_fragment_idx: 0,
            non_contiguous_fragments: BTreeMap::new(),
            payload: vec![],
        }
    }

    /// Returns some if the message has been completely streamed, none otherwise.
    pub fn push_fragment(
        &mut self,
        fragment_number: u32,
        mut fragment: SerializedStream,
    ) -> Option<Vec<u8>> {
        if fragment_number == self.last_contiguous_fragment_idx + 1 {
            self.last_contiguous_fragment_idx = fragment_number;
            self.payload.append(&mut fragment);
        } else {
            self.non_contiguous_fragments
                .insert(fragment_number, fragment);
        }
        while let Some((idx, mut v)) = self.non_contiguous_fragments.pop_first() {
            if idx == self.last_contiguous_fragment_idx + 1 {
                self.last_contiguous_fragment_idx += 1;
                self.payload.append(&mut v);
            } else {
                self.non_contiguous_fragments.insert(idx, v);
                break;
            }
        }
        self.get_and_clear()
    }

    fn get_and_clear(&mut self) -> Option<Vec<u8>> {
        if self.payload.len() as u64 == self.total_length_bytes {
            Some(std::mem::take(&mut self.payload))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InboundStream;

    #[test]
    fn test_simple_sequence() {
        let mut stream = InboundStream::new(6);
        assert_eq!(stream.push_fragment(1, vec![1, 2, 3]), None);
        assert_eq!(
            stream.push_fragment(2, vec![4, 5, 6]),
            Some(vec![1, 2, 3, 4, 5, 6])
        );
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }

    #[test]
    fn test_out_of_order_fragment_1() {
        let mut stream = InboundStream::new(6);
        assert_eq!(stream.push_fragment(1, vec![1, 2]), None);
        assert_eq!(stream.push_fragment(3, vec![5, 6]), None);
        assert_eq!(
            stream.push_fragment(2, vec![3, 4]),
            Some(vec![1, 2, 3, 4, 5, 6])
        );
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }

    #[test]
    fn test_out_of_order_fragment_2() {
        let mut stream = InboundStream::new(6);
        assert_eq!(stream.push_fragment(2, vec![3, 4]), None);
        assert_eq!(stream.push_fragment(3, vec![5, 6]), None);
        assert_eq!(
            stream.push_fragment(1, vec![1, 2]),
            Some(vec![1, 2, 3, 4, 5, 6])
        );
        assert!(stream.non_contiguous_fragments.is_empty());
        assert!(stream.payload.is_empty());
    }
}
