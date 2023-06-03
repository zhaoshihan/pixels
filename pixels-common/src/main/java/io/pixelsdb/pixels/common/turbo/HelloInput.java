package io.pixelsdb.pixels.common.turbo;

public class HelloInput extends Input {
    private String content;

    public HelloInput() {
        super(-1);
    }

    public HelloInput(long transId, String content) {
        super(transId);
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
