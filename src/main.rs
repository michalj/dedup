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
                                        }), //                                    tokio_fs::read(path.clone())
                                            //                                        .map(move |chunk| {
                                            //                                            println!("file: {:?}, size = {}, hash = {}", path, chunk.len(), hash(&chunk));
                                            //                                        })
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
}
