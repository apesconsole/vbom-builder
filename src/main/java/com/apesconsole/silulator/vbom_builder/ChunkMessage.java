package com.apesconsole.silulator.vbom_builder;

public class ChunkMessage {
    public String messageId;
    public int chunkIndex;
    public int totalChunks;
    public String data;

    public ChunkMessage() {
    }

    public ChunkMessage(String messageId, int chunkIndex, int totalChunks, String data) {
        this.messageId = messageId;
        this.chunkIndex = chunkIndex;
        this.totalChunks = totalChunks;
        this.data = data;
    }
}
