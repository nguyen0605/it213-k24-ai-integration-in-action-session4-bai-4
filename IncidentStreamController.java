package com.example.demo.controller;

import org.springframework.ai.chat.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class IncidentStreamController {

    private final ChatModel chatModel;

    public IncidentStreamController(ChatModel chatModel) {
        this.chatModel = chatModel;
    }

    @GetMapping(value = "/api/v1/incident/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> streamIncident(
            @RequestParam String rawMessage,
            @RequestParam(defaultValue = "0.5") Double temp,
            @RequestParam(defaultValue = "1000") Integer maxTokens,
            ServerHttpResponse response) {

        // Bổ sung header HTTP để ngăn chặn Nginx buffer làm tắc nghẽn luồng SSE
        response.getHeaders().add("X-Accel-Buffering", "no");

        // Cấu hình các tùy chọn động dựa vào request params gửi lên
        OpenAiChatOptions options = OpenAiChatOptions.builder()
                .withTemperature(temp)
                .withMaxTokens(maxTokens)
                .build();

        Prompt prompt = new Prompt(rawMessage, options);

        // Streaming phản hồi từ mô hình ngôn ngữ lớn (LLM)
        return chatModel.stream(prompt)
                .map(chatResponse -> {
                    if (chatResponse.getResult() != null && chatResponse.getResult().getOutput() != null) {
                        String content = chatResponse.getResult().getOutput().getContent();
                        return content != null ? content : "";
                    }
                    return "";
                })
                .filter(content -> !content.isEmpty());
    }
}