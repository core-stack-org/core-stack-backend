# Media Upload and Handling System

The Media Upload and Handling System manages the complete lifecycle of media files submitted through bot interfaces—primarily WhatsApp—including receipt, transformation, storage, and association with content moderation workflows. The system processes images and audio through a unified pipeline that converts formats to standardized outputs, stores files in local and S3 locations, and persists metadata in Django models for downstream moderation workflows.

## Architecture Overview

The media handling architecture follows a modular design with distinct responsibilities across ingestion, processing, storage, and persistence layers. When a user submits media through WhatsApp, the webhook receives the message containing a media reference (media\_id and mime\_type), which triggers the download and processing pipeline. The system extracts media from WhatsApp's servers, applies format transformations (e.g., audio conversion to MP3, image resizing), uploads to S3 for long-term storage, and creates database entries linking the media to users and community content for moderation review.

```mermaid
flowchart LR
    A[WhatsApp Media Message] --> B[Webhook Handler]
    B --> C[Extract Media ID & MIME Type]
    C --> D{Media Type?}
    D -->|Image| E[download_image]
    D -->|Audio/Voice| F[download_audio]
    E --> G[download_media_from_url]
    F --> G
    G --> H[Save to Local WHATSAPP_MEDIA_PATH]
    H --> I{Is Audio?}
    I -->|Yes| J[Convert OGG/WAV to MP3]
    I -->|No| K[Convert to HDPI Image]
    J --> L[push_to_s3 Upload]
    K --> L
    L --> M[Create Media DB Entry]
    M --> N[Return S3 URL to Caller]
    style A fill:#e1f5ff
    style N fill:#c8e6c9
```

## Media Ingestion through WhatsApp Webhook

The WhatsApp webhook serves as the entry point for all media submissions. When a user sends media through WhatsApp, the webhook receives an event containing a media object with a `media_id` and `mime_type`. The webhook delegates processing to the `WhatsAppInterface` class, which determines the appropriate handling based on media type. The webhook handler processes both GET requests for verification and POST requests containing message data. For media messages, the system marks messages as read to prevent duplicate processing and extracts the media reference for downstream processing.

Sources: [bot\_interface/api.py](../bot_interface/api.py#L61-L200)

## Media Download from WhatsApp API

The system downloads media files from WhatsApp's servers using their media retrieval API. The `download_media_from_url()` function orchestrates this process by constructing the appropriate download URL using the bot's BSP (Business Solution Provider) credentials and streaming the response to local storage. Each media file is saved using its `media_id` as the filename with the appropriate extension derived from the mime\_type. The download process uses chunked streaming for memory efficiency and includes timeout handling to prevent hanging operations. For audio files, the system immediately converts them to MP3 format during download to ensure standardized audio formats across the platform.

Sources: [bot\_interface/api.py](../bot_interface/api.py#L901-L963), [bot\_interface/api.py](../bot_interface/api.py#L716-L740)

## Media Type Handling

The system distinguishes between different media types and routes them through specialized processing pipelines. Images are processed through the `download_image()` function which retrieves the file from WhatsApp and optionally generates high-density versions for improved display quality. Audio and voice messages are processed through `download_audio()`, which supports various audio formats and converts them to MP3 for consistency. The system automatically detects audio mime types using the `is_audio_file()` helper function and determines the appropriate file extension with `get_audio_extension()`. This abstraction allows the system to handle new media formats by extending the type detection and conversion logic without modifying the core download pipeline.

Audio conversion happens synchronously during the download process, which ensures that by the time a media entry is created in the database, the audio file is already in MP3 format. This design choice simplifies downstream processing since all audio files have consistent format and quality characteristics.

Sources: [bot\_interface/api.py](../bot_interface/api.py#L776-L809), [bot\_interface/api.py](../bot_interface/api.py#L716-L740)

## Image Processing and HDPI Conversion

Images undergo additional processing to optimize them for display across different devices and network conditions. The `convert_image_hdpi()` function creates a high-density version of each image by resizing it to 800×800 pixels while maintaining aspect ratio through Lanczos resampling. This produces a visually optimized version that loads quickly on mobile devices while preserving sufficient quality for moderation review. The HDPI version is stored in a dedicated subdirectory (`hdpi/`) within the WhatsApp media path, and the system returns the path to this optimized version for subsequent S3 upload. This two-tier storage approach maintains both the original image and a display-optimized version, giving the flexibility to use the appropriate file based on context.

Sources: [bot\_interface/utils.py](../bot_interface/utils.py#L1206-L1227)

## Audio Format Conversion

Audio files undergo comprehensive format conversion to ensure compatibility and quality consistency. The system handles OGG format (common in WhatsApp voice messages) by converting it first to WAV using ffmpeg through pydub, then converting the WAV to MP3 at a configurable bitrate (default 192k). This two-step conversion ensures that all audio ends up in MP3 format regardless of the original source format. The conversion process includes detailed logging for debugging and quality verification, logging the input file dimensions, conversion progress, and output file specifications. The `convert_wav_to_mp3()` function implements quality checks to verify successful conversion and provides error handling for edge cases such as file access issues or encoding problems.

Sources: [bot\_interface/api.py](../bot_interface/api.py#L801-L900), [bot\_interface/api.py](../bot_interface/api.py#L861-L900)

## S3 Upload and URL Generation

After local processing, media files are uploaded to Amazon S3 for durable, scalable storage. The `push_to_s3()` function uses boto3 to upload files with the appropriate content type metadata, ensuring browsers display images and audio correctly when served. Files are organized in S3 using a logical directory structure with separate folders for images (`docs/images/`) and audio files (`docs/audios/`). The upload process returns a publicly accessible S3 URL that can be used by the frontend or other services. S3 integration includes error handling with exception catching and returns both success status and error details for troubleshooting failed uploads. The system constructs S3 paths using the original filename from the local storage, maintaining traceability between local processing and cloud storage.

Sources: [bot\_interface/utils.py](../bot_interface/utils.py#L1228-L1260), [bot\_interface/utils.py](../bot_interface/utils.py#L1260-L1270)

## Database Persistence

Processed media files are persisted in the Django database through the `Media` model defined in the community\_engagement app. This model captures essential metadata about each media file including the user who submitted it, the media type (IMAGE, AUDIO, VIDEO, or DOC), the storage path or URL, the source (BOT or IVR), and the bot instance through which it was received. The model supports many-to-many relationships with content items, allowing multiple media files to be associated with a single submission (e.g., multiple photos with one story). The `create_media_entry()` utility function handles the creation of Media records, extracting the necessary information from the user session and media path to create a properly linked database entry.

Sources: [community\_engagement/models.py](../community_engagement/models.py#L72-L81), [bot\_interface/utils.py](../bot_interface/utils.py#L432-L445)

## WhatsApp Interface Integration

The `WhatsAppInterface` class encapsulates media handling logic within the broader bot framework. The `_download_and_upload_media()` static method provides a high-level abstraction that downloads media from WhatsApp, uploads it to S3, and returns the public URL. This method dispatches to type-specific download functions (`_download_image()` or `_download_audio()`) and handles the complete pipeline from WhatsApp retrieval to S3 publication. The `_store_media_data()` method manages the storage of media metadata within user sessions, organizing media files by flow type (e.g., "work\_demand" or "story") and supporting multiple photos per submission. This session-based storage allows the system to collect media across multiple message exchanges before associating them with a final content item for moderation.

Sources: [bot\_interface/interface/whatsapp.py](../bot_interface/interface/whatsapp.py#L194-L217), [bot\_interface/interface/whatsapp.py](../bot_interface/interface/whatsapp.py#L468-L575)

## Media Configuration and Storage Paths

The system uses configurable paths for media storage during processing. The `WHATSAPP_MEDIA_PATH` setting defines the local filesystem location where media files are temporarily stored after download and before S3 upload. This path is automatically created on startup if it doesn't exist, ensuring the system can handle media processing without manual directory setup. The path configuration supports relative and absolute paths, making the system adaptable to different deployment environments. Within this base path, the system creates subdirectories for HDPI images and organized file naming based on media IDs to prevent collisions and maintain traceability throughout the processing pipeline.

Sources: [bot\_interface/api.py](../bot_interface/api.py#L30-L35)

## Error Handling and Resilience

The media handling system includes comprehensive error handling at each stage of the pipeline. Download operations use request timeouts and exception handling to prevent hanging on failed network requests. File operations include try-catch blocks with detailed logging to capture and diagnose processing failures. S3 uploads return detailed error information when failures occur, enabling troubleshooting of credential or network issues. The conversion pipeline includes quality checks and verification steps to detect partial or corrupted conversions before proceeding. Throughout the system, operations return success/failure status codes alongside data, allowing calling code to handle errors gracefully rather than crashing on unexpected conditions. This defensive programming approach ensures that media processing failures don't cascade into system-wide outages.

The processed\_message\_ids set in bot\_interface/api.py prevents duplicate processing of the same media message. This is critical because webhook callbacks may be retried by the platform, and deduplication prevents redundant processing that could waste storage and processing resources.

Sources: [bot\_interface/api.py](../bot_interface/api.py#L901-L963), [bot\_interface/utils.py](../bot_interface/utils.py#L1228-L1260)

## Integration with Content Moderation

While the media handling system focuses on file processing and storage, it provides the foundation for content moderation workflows. Media entries created through this system are associated with content items (stories, grievances, work demands) that undergo moderation review through the moderation module. Moderators can access the S3 URLs stored in Media records to review submissions and make publishing decisions. The separation of concerns between media processing and content moderation allows each system to evolve independently—media handling can support new platforms or formats without changes to moderation logic, and moderation workflows can adjust their criteria without affecting media processing pipelines.

Sources: [community\_engagement/models.py](community_engagement/models.py#L134-L150), [moderation/api.py](moderation/api.py#L48-L75)

## Next Steps

To understand the complete content management ecosystem, explore how uploaded media flows into the [Content Moderation Workflow](/18-content-moderation-workflow)  for review and approval. For deeper understanding of the bot architecture that enables media submissions, refer to the [State Machine Architecture for Bot Conversations](/15-state-machine-architecture-for-bot-conversations). To see how media becomes part of community content, examine the [Community Management System](/17-community-management-system).