
![znippy](https://github.com/user-attachments/assets/7db1c1c1-d577-4f87-bfe1-11af6e8c58a0)

# Znippy (Betha preview !!!!)
Znippy archive format based on Zstandard and Apache Arrow . Built for speed, streaming, and random access,.  

|                 | ⚡ Znippy                                                    | 🐢 tar + zstd                      |
| --------------- | ----------------------------------------------------------- | ---------------------------------- |
| ⚡ Compression   | 44.6 seconds<br>2767 MB → 911 MB<br>418 % compression ratio | 63.3 seconds<br>2767 MB → 722 MB   |
| ⚡ Decompression | 1.9 seconds<br>14979 chunks<br>fully indexed                | 3.9 seconds<br>streamed extraction |



# Snippy is dead — long live Znippy!  
The name Snippy was already in use in the genomics world, so we’re moving forward with Znippy — a name that proudly reflects its foundation in Zstandard (Zstd) and its focus on real-time, high-performance compression. 



## 🧩 Znippy Compression Pipeline – Visual Overview

```mermaid
flowchart LR
    %% Reader + ChunkRevolver
    Reader[Reader Thread] --> Revolver((ChunkRevolver))

    %% Fan-out
    Revolver --> C0[Compressor 0]
    Revolver --> C1[Compressor 1]
    Revolver --> C2[...]
    Revolver --> C31[Compressor 31]

    %% Fan-in
    C0 --> Writer[Writer Thread]
    C1 --> Writer
    C2 --> Writer
    C31 --> Writer

    %% Output and Index
    Writer --> Index[Arrow Index - znippy]
    Writer --> Zdata[zdata File]

    %% Microchunk entries in index
    Index --> M0[Microchunk 0]
    Index --> M1[Microchunk 1]
    Index --> M2[...]
    Index --> M63[Microchunk 63]

    M0 --> Zdata
    M1 --> Zdata
    M2 --> Zdata
    M63 --> Zdata

    %% Final archive
    Index --> Final[Znippy Archive Output]
    Zdata --> Final
