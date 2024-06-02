// use std::{
//     fs::{File, OpenOptions},
//     io::{Error, Write},
//     path::Path,
// };

// use super::NetLogMessage;

// struct SerializedNetLogMessage {
//     data: Vec<u8>,
// }

// pub(super) struct LogFile {
//     file: File,
// }

// impl LogFile {
//     pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
//         let mut file = OpenOptions::new()
//             .create(true)
//             .read(true)
//             .append(true)
//             .open(path)?;
//         todo!()
//     }

//     pub fn append(&mut self, data: &[u8]) -> Result<(), Error> {
//         self.file.write_all(data)
//     }

//     /// Returns number of records
//     pub fn replay(&mut self) -> Result<usize, Error> {
//         let mut len_buf = [0u8; 4];
//     }
// }
