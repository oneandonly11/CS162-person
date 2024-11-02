use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};

use std::path::PathBuf;
use std::path::Path;
use tokio::io::AsyncWriteExt;

use crate::args;

use crate::http::*;
use crate::stats::*;

use clap::Parser;
use tokio::net::{TcpListener, TcpStream};

use anyhow::Result;

pub fn main() -> Result<()> {
    // Configure logging
    // You can print logs (to stderr) using
    // `log::info!`, `log::warn!`, `log::error!`, etc.
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Parse command line arguments
    let args = args::Args::parse();

    // Set the current working directory
    env::set_current_dir(&args.files)?;

    // Print some info for debugging
    log::info!("HTTP server initializing ---------");
    log::info!("Port:\t\t{}", args.port);
    log::info!("Num threads:\t{}", args.num_threads);
    log::info!("Directory:\t\t{}", &args.files);
    log::info!("----------------------------------");

    // Initialize a thread pool that starts running `listen`
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.num_threads)
        .build()?
        .block_on(listen(args.port))
}

async fn listen(port: u16) -> Result<()> {
    // Hint: you should call `handle_socket` in this function.
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)).await?; 

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
             handle_socket(socket).await 
        });
    }

    Ok(())
}

async fn error_404(socket: &mut TcpStream) -> Result<()> {
    let status_code = 404;
    start_response(socket, status_code).await?;
    end_headers(socket).await?;
    Ok(())
}

// Handles a single connection via `socket`.
async fn handle_socket(mut socket: TcpStream) -> Result<()> {
    
    let request = parse_request(&mut socket).await?;
    log::info!("Method: {}, Path: {}", &request.method, &request.path);

    let response = match request.method.as_str() {
        "GET" => handle_get(&request,&mut socket).await?,
        _ => error_404(&mut socket).await?,
    };


    Ok(())
}

// You are free (and encouraged) to add other funtions to this file.
// You can also create your own modules as you see fit.

async fn handle_get(request: &Request, socket: &mut TcpStream) -> Result<()> {

    let path = format!(".{}", request.path);
    let mut path_buf = PathBuf::from(path);
    if path_buf.exists() {
        if path_buf.is_dir() {
            path_buf.push("index.html");
            if path_buf.exists() {
                let path_buf_str = path_buf.into_os_string().into_string().unwrap();
                send_file(socket, &path_buf_str).await?;
            } else {
                let directory = path_buf.parent().unwrap();
                send_directory(socket, &directory).await?;
            }
        }
        else{
            let path_buf_str = path_buf.into_os_string().into_string().unwrap();
            send_file(socket, &path_buf_str).await?;
        }
    } 
    else {
        error_404(socket).await?;
    }
    Ok(())
}



async fn send_file(socket: &mut TcpStream, path: &str ) -> Result<()> {
    let mut file = tokio::fs::File::open(path).await?;
    let status_code = 200;
    start_response(socket, status_code).await?;
    let mime_type = get_mime_type(&path);
    send_header(socket, "Content-Type", &mime_type.to_string()).await?;
    send_header(socket, "Content-Length", &file.metadata().await?.len().to_string()).await?;
    end_headers(socket).await?;
    write_file(socket, &mut file).await?;
    Ok(())
}

async fn send_directory(socket: &mut TcpStream, path: &Path) -> Result<()> {
    start_response(socket, 200).await?;
    let content_type: &str = "text/html";
    let mut html_string = String::from("");
    let mut files = tokio::fs::read_dir(path).await?;
    let parent_str: &str = "..";
    let parent_path = path.parent().unwrap().to_str().unwrap(); 
    
    let parent_link = format_href(&parent_path, &parent_str);
    html_string.push_str(&parent_link);
    while let Some(entry) = files.next_entry().await? {
        let file_str = entry.file_name().into_string().unwrap();
        let path_str = entry.path().into_os_string().into_string().unwrap();
        let link_str = format_href(&path_str, &file_str);
        html_string.push_str(&link_str);
    }
    let content_length: String = html_string.len().to_string();
    send_header(socket, "Content-Type", content_type).await?;
    send_header(socket, "Content-Length", &content_length).await?;
    end_headers(socket).await?;
    socket.write_all(html_string.as_bytes()).await?;
    Ok(())
}


