use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::UnboundedRecvError;
use tokio_fs;

fn main() {
    println!("Hello, world!");
    let (mut sender, receiver) = mpsc::unbounded_channel();
    sender.try_send(".".to_owned());
    let go = receiver
        .for_each(move |from_path| {
            let sender = sender.clone();
            let do_one = tokio_fs::read_dir(from_path)
                .and_then(move |dir| {
                    let sender = sender.clone();
                    dir.for_each(move |entry| {
                        let mut _sender = sender.clone();
                        println!("entry: {:?}", entry.path());
                        let path = entry.path().clone();
                        future::poll_fn(move || entry.poll_file_type()).and_then(move |file_type| {
                            println!(
                                "{:?}: f={:?}, d={:?}",
                                path,
                                file_type.is_file(),
                                file_type.is_dir()
                            );
                            if file_type.is_file() {
                                EitherFuture::Left(
                                    tokio_fs::File::open(path.clone())
                                        .and_then(|handle| hash::first(handle, 1024))
                                        .map(move |h| {
                                            println!("file: {:?}, hash = {}", path, h);
                                        }),
                                )
                            } else {
                                EitherFuture::Right({
                                    let sub_dir = format!("{}", path.to_string_lossy());
                                    _sender.try_send(sub_dir).unwrap();
                                    future::ok(())
                                })
                            }
                        })
                    })
                })
                .map_err(|e| {
                    eprintln!("Error doing one: {:?}", e);
                });
            tokio::spawn(do_one);
            Ok(())
        })
        .map_err(|e| {
            eprintln!("err: {:?}", e);
        });
    tokio::run(go);
}

enum EitherFuture<F1, F2> {
    Left(F1),
    Right(F2),
}

impl<I, E, F1: Future<Item = I, Error = E>, F2: Future<Item = I, Error = E>> Future
    for EitherFuture<F1, F2>
{
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Result<Async<I>, E> {
        match self {
            EitherFuture::Left(f) => f.poll(),
            EitherFuture::Right(f) => f.poll(),
        }
    }
}

mod hash {
    use std::cmp::min;
    use tokio::prelude::*;

    pub fn first<T: AsyncRead>(input: T, bytes: usize) -> HashFuture<T> {
        HashFuture {
            stream: input,
            bytes_left: bytes,
            hash: 0,
        }
    }

    pub struct HashFuture<T> {
        stream: T,
        bytes_left: usize,
        hash: u64,
    }

    impl<T: AsyncRead> Future for HashFuture<T> {
        type Item = u64;
        type Error = std::io::Error;

        fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
            let mut buffer: [u8; 1024] = [0; 1024];
            let read = self.stream.poll_read(&mut buffer);
            match read {
                Err(e) => Err(e),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Ok(Async::Ready(bytes_read)) => {
                    if bytes_read == 0 {
                        Ok(Async::Ready(self.hash))
                    } else {
                        let bytes_to_process = min(self.bytes_left, bytes_read);
                        self.bytes_left -= bytes_to_process;
                        for i in 0..bytes_to_process {
                            self.hash =
                                (self.hash * 21 + buffer[i] as u64 * 27 + 7) % 23436734059613;
                        }
                        if self.bytes_left == 0 {
                            Ok(Async::Ready(self.hash))
                        } else {
                            self.poll()
                        }
                    }
                }
            }
        }
    }

    mod tests {
        use super::*;
        use std::sync::{Arc, Mutex};
        use tokio_fs::File;

        fn hash_file(name: &str, first_bytes: usize) -> u64 {
            // TODO: use tokio-test instead
            let hash_holder: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
            let hash_holder_write = hash_holder.clone();
            let future = File::open(name.to_owned())
                .and_then(move |stream| first(stream, first_bytes))
                .map(move |hash| {
                    hash_holder_write.lock().map(|mut result| {
                        *result = hash;
                    });
                })
                .map_err(|e| panic!(e));
            tokio::run(future);
            let result: u64 = *hash_holder.lock().unwrap();
            result
        }

        #[test]
        fn empty_file_should_hash_to_0() {
            assert_eq!(0, hash_file("test_data/empty_file.txt", 1024));
        }

        #[test]
        fn files_should_have_same_hash_given_same_prefix() {
            assert_eq!(
                hash_file("test_data/test_hash.txt", 3),
                hash_file("test_data/prefix_of_test_hash.txt", 3)
            );
        }
    }
}

mod search {
    use std::path::PathBuf;
    use tokio::prelude::*;

    pub fn files_in_path<P: Into<PathBuf>>(path: P) -> SearchFuture {
        SearchFuture {
            to_visit: vec![path.into()],
            files: vec![],
        }
    }

    pub struct SearchFuture {
        to_visit: Vec<PathBuf>,
        files: Vec<PathBuf>,
    }

    impl Future for SearchFuture {
        type Item = Vec<PathBuf>;
        type Error = std::io::Error;

        fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
            unimplemented!()
        }
    }
}
