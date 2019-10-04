use tokio::prelude::*;
use tokio::sync::mpsc;
use tokio_fs;
use tokio::sync::mpsc::error::UnboundedRecvError;

fn main() {
    println!("Hello, world!");
    let (mut sender, receiver) = mpsc::unbounded_channel();
    sender.try_send(".".to_owned());
    let go = receiver.for_each(move |path| {
        let sender = sender.clone();
        let do_one = tokio_fs::read_dir(path)
            .and_then(move |dir| {
                let sender = sender.clone();
                dir
                    .for_each(move |entry| {
                        let mut _sender = sender.clone();
                        println!("entry: {:?}", entry.path());
                        let path = entry.path().clone();
                        future::poll_fn(move || entry.poll_file_type()).and_then(move |file_type| {
                            println!("{:?}: f={:?}, d={:?}", path, file_type.is_file(), file_type.is_dir());
                            if file_type.is_file() {
                                EitherFuture::Left(
                                    tokio_fs::read(path.clone())
                                        .map(move |chunk| {
                                            println!("file: {:?}, size = {}", path, chunk.len());
                                        })
                                )
                            } else {
                                EitherFuture::Right({
                                    let sub_dir = format!("./{}", path.to_string_lossy());
                                    _sender.try_send(sub_dir).unwrap();
                                    future::ok(())
                                })
                            }
                        })
                    })
            }).map_err(|e| {
                eprintln!("Error doing one: {:?}", e);
            });
        tokio::spawn(do_one);
        Ok(())
    }).map_err(|e| {
        eprintln!("err: {:?}", e);
    });
    tokio::run(go);
}

enum EitherFuture<F1, F2> {
    Left(F1),
    Right(F2),
}

impl<I, E, F1: Future<Item=I, Error=E>, F2: Future<Item=I, Error=E>> Future for EitherFuture<F1, F2> {
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Result<Async<I>, E> {
        match self {
            EitherFuture::Left(f) => f.poll(),
            EitherFuture::Right(f) => f.poll()
        }
    }
}
