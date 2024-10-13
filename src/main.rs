use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    num::NonZero,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
    time::Instant,
};

use clap::Parser;
use crossbeam_channel::Receiver;
use json_writer::JSONObjectWriter;
use wikiwho::{
    algorithm::Analysis,
    dump_parser::{Contributor, DumpParser, Page},
};

#[derive(Debug, clap::Parser)]
struct CommandLine {
    input_file: PathBuf,
}

fn main() {
    // init tracing logging
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let args: CommandLine = CommandLine::parse();

    let file = File::open(&args.input_file)
        .unwrap_or_else(|_| panic!("file not found: {}", args.input_file.display()));
    let reader = zstd::stream::Decoder::new(file).unwrap();
    let reader = BufReader::new(reader);

    let mut parser = DumpParser::new(reader).expect("Failed to create parser");
    eprintln!("Site info: {:?}", parser.site_info());

    let mut threads = Vec::new();

    let num_threads = std::thread::available_parallelism()
        .map(NonZero::get)
        .unwrap_or(1);
    let num_threads = usize::max(1, num_threads - 1);
    let (sender, receiver) = crossbeam_channel::bounded(num_threads * 4);

    for _ in 0..num_threads {
        // spawn worker thread
        let receiver: Receiver<Page> = receiver.clone();

        let thread = std::thread::spawn(move || {
            let mut output_buffer = String::new();
            for page in receiver {
                // if page.namespace != 0 {
                //     continue;
                // }

                let analysis = match Analysis::analyse_page(&page.revisions) {
                    Ok(analysis) => analysis,
                    Err(err) => {
                        tracing::error!(message = "Failed to analyse page, skipping.", error = ?err, title = ?page.title);
                        tracing::debug!(?page);
                        continue;
                    }
                };
                let latest_rev_id = *analysis.ordered_revisions.last().unwrap();
                let latest_rev_pointer = analysis.revisions_by_id[&latest_rev_id].clone();

                let mut author_contributions = HashMap::new();
                for word_pointer in
                    wikiwho::utils::iterate_revision_tokens(&analysis, &latest_rev_pointer)
                {
                    let origin_rev_id = analysis[word_pointer].origin_rev_id;
                    let origin_rev = &analysis.revisions_by_id[&origin_rev_id];

                    let author = origin_rev.xml_revision.contributor.clone();
                    let author_contribution = author_contributions.entry(author).or_insert(0);
                    *author_contribution += 1;
                }

                // Find top 5 authors and everyone with at least 5% of the total contributions or at least 25 tokens
                /*
                total_contributions = sum(author_contributions.values())
                top_authors = sorted(author_contributions.items(), key=lambda x: x[1], reverse=True)[:5]
                top_authors += filter(lambda x: (x[1] / total_contributions >= 0.05 or x[1] >= 25) and not (x in top_authors), author_contributions.items())
                */
                let total_contributions: usize = author_contributions.values().sum();
                let mut top_authors: Vec<(&Contributor, &usize)> =
                    author_contributions.iter().collect();
                top_authors
                    .sort_by(|a, b| b.1.cmp(a.1).then_with(|| b.0.username.cmp(&a.0.username))); /* note reversed order on name comparison to match python script */
                top_authors.truncate(5);
                top_authors.extend(author_contributions.iter().filter(|(_, count)| {
                    **count as f64 / total_contributions as f64 >= 0.05 || **count >= 25
                }));
                top_authors.sort_by(|a, b| {
                    a.0.id
                        .cmp(&b.0.id)
                        .then_with(|| a.0.username.cmp(&b.0.username))
                });
                top_authors.dedup();
                top_authors
                    .sort_by(|a, b| b.1.cmp(a.1).then_with(|| b.0.username.cmp(&a.0.username)));

                let mut object_writer = JSONObjectWriter::new(&mut output_buffer);

                object_writer.value("page", page.title.as_str());
                object_writer.value("ns", page.namespace);
                let mut array_writer = object_writer.array("top_authors");
                for (author, count) in top_authors {
                    let mut author_writer = array_writer.object();
                    author_writer.value("id", author.id);
                    author_writer.value("text", author.username.as_str());
                    author_writer.value("contributions", *count as u64);
                }
                array_writer.end();
                object_writer.value("total_tokens", total_contributions as u64);

                // let mut array_writer = object_writer.array("current_tokens");
                // for word in wikiwho::utils::iterate_revision_tokens(&analysis, &latest_rev_pointer) {
                //     let mut token = array_writer.object();
                //     token.value("t", word.value.as_str());
                //     token.value("r", analysis[word].origin_rev_id);
                //     // array_writer.value(word.value.as_str());
                // }
                // array_writer.end();

                object_writer.end();

                println!("{output_buffer}");
                output_buffer.clear();
            }
        });

        threads.push(thread);
    }

    let mut page_iter = std::iter::from_fn(move || {
        parser
            .parse_page()
            .expect("Failed to parse page")
            .map(|page| (page, parser.bytes_consumed()))
    });

    let curr_bytes_consumed = Arc::new(AtomicU64::new(0));

    let producer_thread = std::thread::spawn({
        let curr_bytes_consumed = curr_bytes_consumed.clone();
        move || {
            while let Some((page, bytes_consumed)) = page_iter.next() {
                curr_bytes_consumed.store(bytes_consumed, std::sync::atomic::Ordering::Relaxed);
                sender.send(page).expect("Failed to send page");
            }
        }
    });

    let mut last_status = Instant::now();

    while !producer_thread.is_finished() {
        if last_status.elapsed().as_secs() >= 10 {
            // log progress approx. every 10 seconds
            tracing::info!(
                "Consumed uncompressed data: {:.2}",
                byte_unit::Byte::from_u64(
                    curr_bytes_consumed.load(std::sync::atomic::Ordering::Relaxed)
                )
                .get_appropriate_unit(byte_unit::UnitType::Binary)
            );
            last_status = Instant::now();
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    for thread in threads {
        thread.join().expect("Failed to join thread");
    }
}
