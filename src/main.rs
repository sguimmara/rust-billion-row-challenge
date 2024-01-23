use std::path::Path;

use clap::Parser;

mod parser;
mod processor;
mod reader;

use parser::{NaiveRowParser, VectorizedRowParser};
use processor::{NoopProcessor, ParallelRayonProcessor, Processor, SequentialProcessor, Station};
use reader::{ChunkReader, MemoryMappedReader};

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
    ParallelRayon,
    NoOp,
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

    /// How the input CSV file is parsed into rows
    #[arg(long)]
    reader: ReaderType,

    /// How the input CSV file is parsed into rows
    #[arg(long)]
    parser: ParserType,

    /// How the CSV rows are processed.
    #[arg(long)]
    processor: ProcessorType,
}

fn run_with_sequential_processor(path: &Path, args: &Args) -> Vec<Station> {
    match (args.parser, args.reader) {
        (ParserType::Naive, ReaderType::Chunk) => {
            SequentialProcessor::<ChunkReader<NaiveRowParser>>::new(path).process()
        }
        (ParserType::Naive, ReaderType::MemoryMapped) => {
            SequentialProcessor::<MemoryMappedReader<NaiveRowParser>>::new(path).process()
        }
        (ParserType::Vectorized, ReaderType::Chunk) => {
            SequentialProcessor::<ChunkReader<VectorizedRowParser>>::new(path).process()
        }
        (ParserType::Vectorized, ReaderType::MemoryMapped) => {
            SequentialProcessor::<MemoryMappedReader<VectorizedRowParser>>::new(path).process()
        }
    }
}

fn run_with_no_op_processor(path: &Path, args: &Args) -> Vec<Station> {
    match (args.parser, args.reader) {
        (ParserType::Naive, ReaderType::Chunk) => {
            NoopProcessor::<ChunkReader<NaiveRowParser>>::new(path).process()
        }
        (ParserType::Naive, ReaderType::MemoryMapped) => {
            NoopProcessor::<MemoryMappedReader<NaiveRowParser>>::new(path).process()
        }
        (ParserType::Vectorized, ReaderType::Chunk) => {
            NoopProcessor::<ChunkReader<VectorizedRowParser>>::new(path).process()
        }
        (ParserType::Vectorized, ReaderType::MemoryMapped) => {
            NoopProcessor::<MemoryMappedReader<VectorizedRowParser>>::new(path).process()
        }
    }
}

fn run_with_parallel_rayon_processor(path: &Path, args: &Args) -> Vec<Station> {
    match (args.parser, args.reader) {
        (ParserType::Naive, ReaderType::Chunk) => {
            ParallelRayonProcessor::<ChunkReader<NaiveRowParser>>::new(path).process()
        }
        (ParserType::Naive, ReaderType::MemoryMapped) => {
            ParallelRayonProcessor::<MemoryMappedReader<NaiveRowParser>>::new(path).process()
        }
        (ParserType::Vectorized, ReaderType::Chunk) => {
            ParallelRayonProcessor::<ChunkReader<VectorizedRowParser>>::new(path).process()
        }
        (ParserType::Vectorized, ReaderType::MemoryMapped) => {
            ParallelRayonProcessor::<MemoryMappedReader<VectorizedRowParser>>::new(path).process()
        }
    }
}

fn main() {
    let args = Args::parse();

    let path = std::path::Path::new(&args.input);

    if !path.exists() {
        eprintln!("no such file: {}", args.input);
        std::process::exit(1);
    }

    let results = match args.processor {
        ProcessorType::Sequential => run_with_sequential_processor(path, &args),
        ProcessorType::ParallelRayon => run_with_parallel_rayon_processor(path, &args),
        ProcessorType::NoOp => run_with_no_op_processor(path, &args),
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
