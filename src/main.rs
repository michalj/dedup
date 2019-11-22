use async_std::{
    fs::File,
    net::{TcpListener, ToSocketAddrs},
    prelude::*,
    task,
};
use std::collections::HashMap;
use std::path::PathBuf;

fn main() {
    task::block_on(async {
        let mut files = search::search("test_data");
        while let Some(item) = files.next().await {
            let input = File::open(&item).await.unwrap();
            let h = hash::first(input, 10).await.unwrap();
            println!("item: {:?}, hash: {}", item, h);
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
    use async_std::{
        fs::File,
        net::{TcpListener, ToSocketAddrs},
        prelude::*,
        task,
    };
    use futures::AsyncRead;

    pub async fn compare<R1: AsyncRead + Unpin, R2: AsyncRead + Unpin>(
        mut stream_1: R1,
        mut stream_2: R2,
    ) -> Result<bool, std::io::Error> {
        let mut buffer_1: [u8; 1024] = [0; 1024];
        let mut buffer_2: [u8; 1024] = [0; 1024];
        loop {
            let read_1 = read_as_much(&mut stream_1, &mut buffer_1).await?;
            let read_2 = read_as_much(&mut stream_2, &mut buffer_2).await?;
            if read_1 != read_2 {
                return Ok(false);
            }
            if read_1 == 0 {
                return Ok(true);
            }
            if buffer_1[..read_1] != buffer_2[..read_1] {
                return Ok(false);
            }
        }
    }

    async fn read_as_much<R: AsyncRead + Unpin>(
        input: &mut R,
        buffer: &mut [u8],
    ) -> Result<usize, std::io::Error> {
        let mut bytes_read = 0;
        loop {
            if bytes_read == buffer.len() {
                break;
            }
            let r = input.read(&mut buffer[bytes_read..]).await?;
            if r == 0 {
                break;
            }
            bytes_read += r;
        }
        Ok(bytes_read)
    }

    mod tests {
        use super::*;
        use async_std::{
            fs::File,
            net::{TcpListener, ToSocketAddrs},
            prelude::*,
            task,
        };

        fn compare_sync(file_1: &str, file_2: &str) -> bool {
            task::block_on(async {
                compare(
                    File::open(file_1).await.unwrap(),
                    File::open(file_2).await.unwrap(),
                )
                .await
                .unwrap()
            })
        }

        #[test]
        fn same_prefix_different_file() {
            assert_eq!(
                compare_sync(
                    "test_data/test_hash.txt",
                    "test_data/prefix_of_test_hash.txt"
                ),
                false
            );
        }

        #[test]
        fn same_file() {
            assert_eq!(
                compare_sync("test_data/test_hash.txt", "test_data/test_hash.txt"),
                true
            );
        }

        #[test]
        fn same_content_file() {
            assert_eq!(
                compare_sync(
                    "test_data/test_hash.txt",
                    "test_data/sub_directory/copy_of_test_hash.txt"
                ),
                true
            );
        }
    }
}

mod hash {
    use async_std::{fs, fs::File, io, prelude::*, task};
    use futures::AsyncRead;
    use std::cmp;

    pub async fn first<T: AsyncRead + Unpin>(
        mut input: T,
        bytes: usize,
    ) -> Result<u64, std::io::Error> {
        let mut buffer: [u8; 1024] = [0; 1024];
        let mut hash: u64 = 0;
        let mut bytes_to_process = bytes;
        loop {
            let bytes_read = input.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            let bytes_to_hash = cmp::min(bytes_to_process, bytes_read);
            for i in 0..bytes_to_hash {
                hash = (hash * 21 + buffer[i] as u64 * 27 + 7) % 23436734059613;
            }
            bytes_to_process -= bytes_to_hash;
            if bytes_to_process == 0 {
                break;
            }
        }
        Ok(hash)
    }

    mod tests {
        use super::*;
        use async_std::{fs, fs::File, io, prelude::*, task};

        fn hash_file(name: &str, first_bytes: usize) -> u64 {
            task::block_on(async {
                let mut input = File::open(name.to_owned()).await.unwrap();
                first(input, first_bytes).await.unwrap()
            })
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
    use async_std::{fs, fs::File, io, prelude::*, task};
    use futures::channel::mpsc;
    use futures::sink::SinkExt;
    use futures::Stream;
    use std::collections::VecDeque;
    use std::path::PathBuf;

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
        use async_std::{fs, fs::File, io, prelude::*, task};
        use futures::stream::StreamExt;
        use std::sync::{Arc, Mutex};

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
