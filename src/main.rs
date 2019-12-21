use async_std::{
    fs::File,
    prelude::*,
    task,
};
use futures::join;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use structopt::StructOpt;
use log::*;

#[derive(StructOpt)]
struct Cli {
    /// Base directory
    #[structopt(parse(from_os_str))]
    base_directory: std::path::PathBuf,
    /// Directory in which to look for duplicates
    #[structopt(parse(from_os_str))]
    new_directory: std::path::PathBuf,
    /// Length of prefix to be hashed
    #[structopt(short = "h", default_value = "512")]
    hash_prefix: usize,
    /// Ignore empty files
    #[structopt(short = "i")]
    ignore_empty_files: bool,
    /// Show original files instead of duplicates
    #[structopt(short = "o")]
    show_original_files: bool,
}

fn main() {
    let args = Cli::from_args();

    task::block_on(async {
        let files = search::search(args.base_directory, args.ignore_empty_files);
        let files_by_hash = files_by_hash(files, args.hash_prefix).await;
        println!("done indexing, {} entries", files_by_hash.len());
        debug!("by_hash: {:?}", files_by_hash);
        let mut new_files = search::search(args.new_directory, args.ignore_empty_files);
        while let Some(item) = new_files.next().await {
            let input = File::open(&item).await.unwrap();
            let h = hash::first(input, args.hash_prefix).await.unwrap();
            match files_by_hash.get(&h) {
                None => {
                    if args.show_original_files {
                        println!("original file: {:?}", item);
                    }
                }
                Some(files_with_same_hash) => {
                    let mut has_duplicate = false;
                    for file in files_with_same_hash {
                        if file != &item {
                            debug!("potential duplicate: {:?} vs {:?}", file, item);
                            let (i1, i2) = join!(File::open(file), File::open(&item));
                            let result = compare::compare(i1.unwrap(), i2.unwrap()).await.unwrap();
                            debug!("compare = {:?}", result);
                            if result {
                                has_duplicate = true;
                                if !args.show_original_files {
                                    println!("duplicate: {:?} vs {:?}", file, item);
                                }
                            }
                        }
                    }
                    if args.show_original_files && !has_duplicate {
                        println!("original file: {:?}", item);
                    }
                }
            }
        }
    });
}

async fn files_by_hash<T>(mut files: T, hash_prefix: usize) -> HashMap<u64, Vec<PathBuf>> where T: Stream<Item = PathBuf> + Unpin {
    let mut files_by_hash: HashMap<u64, Vec<PathBuf>> = HashMap::new();
    while let Some(item) = files.next().await {
         match File::open(&item).await {
             Ok(input) => {
                 let h = hash::first(input, hash_prefix).await.unwrap();
                 debug!("item: {:?}, hash: {}", item, h);
                 match files_by_hash.entry(h) {
                     Entry::Vacant(entry) => {
                         entry.insert(vec![item]);
                     }
                     Entry::Occupied(mut entry) => {
                         entry.get_mut().push(item);
                     }
                 }
             },
             Err(e) => {
                 eprintln!("Cannot open file {:?}, skipping. Reason: {:?}", item, e);
             }
         }
    }
    files_by_hash
}

mod compare {
    use async_std::prelude::*;
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

    #[cfg(test)]
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
    use async_std::prelude::*;
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

    #[cfg(test)]
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
    use async_std::{fs, prelude::*, task};
    use futures::channel::mpsc;
    use futures::sink::SinkExt;
    use futures::Stream;
    use std::collections::VecDeque;
    use std::path::PathBuf;

    pub fn search<P: Into<PathBuf>>(path: P, ignore_empty_files: bool) -> impl Stream<Item = PathBuf> {
        let path = path.into();
        let (mut sender, receiver) = mpsc::unbounded();
        task::spawn(async move {
            let mut queue = VecDeque::new();
            queue.push_back(path);
            while let Some(path) = queue.pop_front() {
                match fs::read_dir(&path).await {
                    Ok(mut dir) => {
                        while let Some(res) = dir.next().await {
                            let entry = res.unwrap();
                            let path = entry.path();
                            let file_type = entry.file_type().await.unwrap();
                            if file_type.is_dir() {
                                queue.push_back(path.into());
                            } else if file_type.is_file() {
                                let ignore_file = if ignore_empty_files {
                                    let meta = entry.metadata().await.unwrap();
                                    meta.len() == 0
                                } else {
                                    false
                                };
                                if !ignore_file {
                                    sender.send(path.into()).await.unwrap();
                                }
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Could not read directory: {:?}. Reason: {:?}", path, e);
                    }
                }
            }
        });
        receiver
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use async_std::{fs, fs::File, io, prelude::*, task};
        use futures::stream::StreamExt;
        use std::sync::{Arc, Mutex};

        #[test]
        fn search_test_data() {
            let output: Vec<PathBuf> = task::block_on(search("test_data/", false).collect());
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
