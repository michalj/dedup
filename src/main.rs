use std::collections::HashMap;
use std::path::PathBuf;
use async_std::{
    prelude::*,
    task,
    net::{TcpListener, ToSocketAddrs},
};

fn main() {
    task::block_on(async {
        let mut files = search::search("test_data");
        while let Some(item) = files.next().await {
            println!("item: {:?}", item);
        }
        println!("end of input");
    });
//    let base_path = "test_data";
//    const FIRST_N_BYTES: usize = 10;
//    let paths_by_hash = search::files_in_path(base_path)
//        .and_then(|path| {
//            tokio_fs::File::open(path.clone())
//                .and_then(|handle| hash::first(handle, FIRST_N_BYTES).map(|h| (path, h)))
//        })
//        .collect()
//        .map(|result| {
//            let mut paths_by_hash: HashMap<u64, Vec<PathBuf>> = HashMap::new();
//            for (path, h) in result {
//                if !paths_by_hash.contains_key(&h) {
//                    paths_by_hash.insert(h, vec![]);
//                }
//                paths_by_hash.get_mut(&h).unwrap().push(path);
//            }
//            paths_by_hash
//        });
//    let go = paths_by_hash
//        .and_then(move |map| {
//            search::files_in_path(base_path).and_then(|path| {
//                tokio_fs::File::open(path.clone()).and_then(|handle| {
//                    hash::first(handle, FIRST_N_BYTES).map(|h| {
//                        (path, h)
//                    })
//                })
//            }).collect().map(move |files| {
//                for (path, h) in files {
//                    if let Some(entries) = map.get(&h) {
//                        for entry in entries {
//                            if entry != &path {
//                                println!("potential duplicate: {:?} vs {:?}", path, entry);
//
//                            }
//                        }
//                    }
//                }
//            })
//        })
//        .map(|result| {
//            println!("result: {:?}", result);
//        })
//        .map_err(|e| {
//            println!("error: {:?}", e);
//        });
//    task::block_on(go);
//    //tokio::run(go);
}

mod compare {
//    use tokio::prelude::*;
//    use tokio::codec;
//
//    fn compare<R1, R2>(stream_a: R1, stream_b: R2) -> impl Future<Item = bool, Error = std::io::Error> where R1: AsyncRead + 'static, R2: AsyncRead + 'static {
//        let s_a = codec::FramedRead::new(stream_a, codec::BytesCodec::new())
//            .map(|bytes| stream::iter_ok(bytes))
//            .flatten();
//        let s_b = codec::FramedRead::new(stream_b, codec::BytesCodec::new())
//            .map(|bytes| stream::iter_ok(bytes))
//            .flatten();
//        let combined = s_a.zip(s_b);
//        CompareFuture {
//            combined: Box::new(combined)
//        }
//    }
//
//    pub struct CompareFuture {
//        combined: Box<dyn Stream<Item = (u8, u8), Error = std::io::Error>>,
//    }
//
//    impl Future for CompareFuture {
//        type Item = bool;
//        type Error = std::io::Error;
//
//        fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
//            match self.combined.poll() {
//                Ok(Async::Ready(Some((a, b)))) => {
//                    if a != b {
//                        Ok(Async::Ready(false))
//                    } else {
//                        self.poll()
//                    }
//                },
//                Ok(Async::Ready(None)) => Ok(Async::Ready(true)),
//                Ok(Async::NotReady) => Ok(Async::NotReady),
//                Err(err) => Err(err),
//            }
//        }
//    }
//
//    mod tests {
//        use super::*;
//        use tokio_fs;
//
//        #[test]
//        fn same_prefix_different_file() {
//            let result = tokio_fs::File::open("test_data/test_hash.txt").join(tokio_fs::File::open("test_data/prefix_of_test_hash.txt")).and_then(|(f1, f2)| {
//                compare(f1, f2)
//            });
//
//        }
//    }
}

mod hash {
//    use std::task::{Context, Poll};
//    use std::cmp::min;
//    use tokio::prelude::*;
//
//    pub fn first<T: AsyncRead>(input: T, bytes: usize) -> HashFuture<T> {
//        HashFuture {
//            stream: input,
//            bytes_left: bytes,
//            hash: 0,
//        }
//    }
//
//    pub struct HashFuture<T> {
//        stream: T,
//        bytes_left: usize,
//        hash: u64,
//    }
//
//    impl<T: AsyncRead> Future for HashFuture<T> {
//        type Output = Result<u64, std::io::Error>;
//
//        fn poll(self, cx: &mut Context) -> Poll<Self::Output> {
//            unimplemented!()
//        }
//
////        fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
////            let mut buffer: [u8; 1024] = [0; 1024];
////            let read = self.stream.poll_read(&mut buffer);
////            match read {
////                Ok(Async::Ready(bytes_read)) => {
////                    if bytes_read == 0 {
////                        Ok(Async::Ready(self.hash))
////                    } else {
////                        let bytes_to_process = min(self.bytes_left, bytes_read);
////                        self.bytes_left -= bytes_to_process;
////                        for i in 0..bytes_to_process {
////                            self.hash =
////                                (self.hash * 21 + buffer[i] as u64 * 27 + 7) % 23436734059613;
////                        }
////                        if self.bytes_left == 0 {
////                            Ok(Async::Ready(self.hash))
////                        } else {
////                            self.poll()
////                        }
////                    }
////                }
////                Ok(Async::NotReady) => Ok(Async::NotReady),
////                Err(e) => Err(e),
////            }
////        }
//    }
//
//    mod tests {
//        use super::*;
//        use std::sync::{Arc, Mutex};
//        use tokio_fs::File;
//
//        fn hash_file(name: &str, first_bytes: usize) -> u64 {
//            // TODO: use tokio-test instead
//            let hash_holder: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
//            let hash_holder_write = hash_holder.clone();
//            let future = File::open(name.to_owned())
//                .and_then(move |stream| first(stream, first_bytes))
//                .map(move |hash| {
//                    hash_holder_write.lock().map(|mut result| {
//                        *result = hash;
//                    });
//                })
//                .map_err(|e| panic!(e));
//            tokio::run(future);
//            let result: u64 = *hash_holder.lock().unwrap();
//            result
//        }
//
//        #[test]
//        fn empty_file_should_hash_to_0() {
//            assert_eq!(0, hash_file("test_data/empty_file.txt", 1024));
//        }
//
//        #[test]
//        fn files_should_have_same_hash_given_same_prefix() {
//            assert_eq!(
//                hash_file("test_data/test_hash.txt", 3),
//                hash_file("test_data/prefix_of_test_hash.txt", 3)
//            );
//        }
//    }
}

mod search {
    use async_std::{fs, fs::File, io, prelude::*, task};
    use futures::Stream;
    use futures::channel::mpsc;
    use futures::sink::SinkExt;
    use std::path::PathBuf;
    use std::collections::VecDeque;

    pub fn search<P: Into<PathBuf>>(path: P) -> impl Stream<Item = PathBuf> {
        let path = path.into();
        let (mut sender, receiver) = mpsc::unbounded();
        task::spawn(async move {
            let mut queue = VecDeque::new();
            queue.push_back(path);
            while let Some(path) = queue.pop_front() {
                let mut dir = fs::read_dir(path).await.unwrap();
                while let Some(res) = dir.next().await {
                    let entry = res.unwrap();
                    let path = entry.path();
                    let file_type = entry.file_type().await.unwrap();
                    if file_type.is_dir() {
                        queue.push_back(path.into());
                    } else if file_type.is_file() {
                        sender.send(path.into()).await.unwrap();
                    }
                }
            }
        });
        receiver
    }

    mod tests {
        use super::*;
        use std::sync::{Arc, Mutex};
        use async_std::{fs, fs::File, io, prelude::*, task};
        use futures::stream::StreamExt;

        #[test]
        fn search_test_data() {
            let output: Vec<PathBuf> = task::block_on(search("test_data/").collect());
            assert_eq!(
                output,
                vec![
                    PathBuf::from("test_data/prefix_of_test_hash.txt"),
                    PathBuf::from("test_data/empty_file.txt"),
                    PathBuf::from("test_data/test_hash.txt"),
                    PathBuf::from("test_data/sub_directory/copy_of_test_hash.txt"),
                ]
            );
        }
    }
}
