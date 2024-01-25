use clap::Parser;

mod brc;

use brc::{
    processing::{parallel_channel::ParallelChannel, sequential::Sequential, Processor},
    reader::{mmap::MmapReader, Reader},
};

#[derive(clap::ValueEnum, Copy, Clone, Default, Debug, PartialEq)]
#[clap(rename_all = "kebab_case")]
enum ParserType {
    #[default]
    Naive,
    Vectorized,
}

#[derive(clap::ValueEnum, Copy, Clone, Default, Debug, PartialEq)]
#[clap(rename_all = "kebab_case")]
enum ReaderType {
    #[default]
    Chunk,
    MemoryMapped,
}

#[derive(clap::ValueEnum, Copy, Clone, Default, Debug, PartialEq)]
#[clap(rename_all = "kebab_case")]
enum ProcessorType {
    #[default]
    Sequential,
    ParallelChannel,
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The file to read.
    input: String,

    /// Don't print anything.
    #[arg(short, long)]
    quiet: bool,
    // /// How the input CSV file is parsed into rows
    // #[arg(long)]
    // reader: ReaderType,

    // /// How the input CSV file is parsed into rows
    // #[arg(long)]
    // parser: ParserType,
    /// How the CSV rows are processed.
    #[arg(long)]
    processor: ProcessorType,
}

fn main() {
    let args = Args::parse();

    let path = std::path::Path::new(&args.input);

    if !path.exists() {
        eprintln!("no such file: {}", args.input);
        std::process::exit(1);
    }

    let results = match args.processor {
        ProcessorType::Sequential => Sequential::<MmapReader>::new(MmapReader::new(path)).process(),
        ProcessorType::ParallelChannel => ParallelChannel::<MmapReader>::new(path).process(),
    };

    if !args.quiet {
        print!("{{");
        let mut first = true;
        for r in results {
            if !first {
                print!(", ");
            }
            first = false;
            print!("{}={}/{}/{}", r.name, r.min, r.mean, r.max);
        }
        println!("}}");
    }
}
