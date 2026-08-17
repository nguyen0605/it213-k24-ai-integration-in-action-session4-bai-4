# BÁO CÁO BÀI TẬP 4: XÂY DỰNG API STREAM WEBFLUX VỚI DYNAMIC CHATOPTIONS

- **Họ và tên:** Nguyễn Văn A
- **Mã học viên:** nguyen0605_PTIT-HCM-059
- **Môn học:** Kỹ năng ứng dụng AI

---

## PHẦN 1: TIÊU ĐỀ BÀI TẬP VÀ YÊU CẦU ĐỀ BÀI

### Tiêu đề
**Xây dựng API Stream WebFlux với Dynamic ChatOptions hỗ trợ Logistics Real-time Incident Analysis**

### Yêu cầu đề bài
1. **Thiết lập API Endpoint Reactive:** Triển khai một REST Controller `IncidentStreamController` cung cấp API `/api/v1/incident/stream` nhận vào một chuỗi `rawMessage` chứa báo cáo sự cố kèm các tham số động `temp` (mặc định `0.5`) và `maxTokens` (mặc định `1000`).
2. **Hỗ trợ Dynamic ChatOptions:** Áp dụng `OpenAiChatOptions` để cấu hình động cho mỗi yêu cầu mà không làm thay đổi cấu hình mặc định của hệ thống.
3. **Ngăn chặn Buffer Proxy:** Tích hợp tiêu đề HTTP `X-Accel-Buffering: no` để đảm bảo luồng Server-Sent Events (SSE) luôn chảy mượt mà xuyên suốt qua các reverse proxy trung gian như Nginx.
4. **Phân tích hiệu năng chuyên sâu:** So sánh sự khác biệt lớn về kiến trúc Thread Pool, mức tiêu thụ tài nguyên và khả năng mở rộng giữa Reactive Stream WebFlux và Synchronous Spring Web MVC khi xử lý luồng phản hồi token thời gian thực từ LLMs.

---

## PHẦN 2: GIẢ LẬP NỘI DUNG CUỘC TRÒ CHUYỆN VỚI AI TOÀN DIỆN

### 💬 Prompt gửi cho AI:

> "Chào bạn, tôi là một lập trình viên Java backend đang xây dựng hệ thống phân tích sự cố Logistics cho ban điều hành. Tôi cần viết một API streaming thời gian thực bằng Java Spring Boot sử dụng Spring WebFlux kết hợp với Spring AI. 
>
> API của tôi phải đáp ứng các tiêu chuẩn kỹ thuật sau:
> 1. Sử dụng Spring WebFlux trả về luồng `Flux<String>` kiểu SSE (`text/event-stream`).
> 2. Có endpoint là `/api/v1/incident/stream` nhận tham số `rawMessage` và các config động: `temp` (default 0.5), `maxTokens` (default 1000) qua request parameters.
> 3. Cần custom ghi đè cấu hình cho từng request thông qua `OpenAiChatOptions`.
> 4. Do API chạy phía sau proxy Nginx, tôi cần cấu hình header `X-Accel-Buffering: no` ngay tại tầng phản hồi WebFlux.
> 5. Hãy phân tích chuyên sâu cho tôi sự khác biệt về mặt hiệu năng, Thread Pool giữa mô hình Non-blocking (WebFlux) và Blocking (Web MVC) khi gọi LLM API dạng stream. Đồng thời cung cấp đầy đủ mã nguồn Java và file cấu hình Maven `pom.xml` hoàn chỉnh."

### 🤖 AI Response phản hồi chi tiết:

> "Chào bạn! Dưới đây là giải pháp kiến trúc toàn diện và mã nguồn chi tiết để đáp ứng yêu cầu của Ban điều hành Logistics. 
>
> ### 1. Kiến Trúc Giải Pháp
> Chúng tôi sẽ sử dụng **Spring WebFlux** tích hợp **Spring AI OpenAI Starter**. Để cấu hình động các thông số cho từng request riêng biệt, chúng ta truyền một instance `OpenAiChatOptions` trực tiếp vào đối tượng `Prompt`. Để tránh Nginx đệm (buffer) dữ liệu SSE, chúng tôi can thiệp vào `ServerHttpResponse` để thiết lập header HTTP `X-Accel-Buffering: no` trước khi bắt đầu stream.
>
> ### 2. Mã nguồn triển khai
> *(Xem chi tiết file `IncidentStreamController.java` và `pom.xml` trong thư mục project)*
>
> ### 3. Phân Tích Chuyên Sâu Hiệu Năng (WebFlux vs Web MVC)
> Khi streaming tokens từ LLM (vốn là một tác vụ I/O kéo dài, mất vài giây đến vài chục giây):
> - **Spring Web MVC (Blocking - Thread-per-request):** Mỗi request chiếm dụng hoàn toàn 1 Thread của Tomcat (ví dụ trong pool 200 threads). Nếu có 200 yêu cầu đồng thời phân tích sự cố kéo dài, hệ thống sẽ cạn kiệt luồng (Thread Starvation) và từ chối các kết nối tiếp theo.
> - **Spring WebFlux (Non-blocking - Event Loop):** Chỉ sử dụng một số lượng thread cực nhỏ (thường bằng số nhân CPU) của Netty. Khi đợi phản hồi từ OpenAI API, thread được giải phóng ngay lập tức để phục vụ yêu cầu khác. Do đó, WebFlux có khả năng chịu tải cao hơn gấp hàng chục lần so với Web MVC với lượng tiêu thụ tài nguyên cực kỳ tối giản."

---

## PHẦN 3: PHÂN TÍCH CHUYÊN SÂU HIỆU NĂNG: WEBFLUX VS WEB MVC KHI STREAMING TOKENS

| Đặc tính so sánh | Spring Web MVC (Đồng bộ / Blocking) | Spring WebFlux (Phản ứng / Non-blocking) |
| :--- | :--- | :--- |
| **Kiến trúc Thread** | **Thread-per-request**: Mỗi request streaming sẽ chiếm trọn 1 thread trong Tomcat Thread Pool từ đầu đến cuối phiên kết nối. | **Event Loop**: Netty quản lý yêu cầu bằng vòng lặp sự kiện. Thread xử lý không bị chặn khi đợi dữ liệu từ LLM API trả về. |
| **Giới hạn chịu tải (Concurrency)** | Bị giới hạn trực tiếp bởi số lượng tối đa của Thread Pool (mặc định Tomcat là 200). Khi vượt tải, các request sau sẽ xếp hàng hoặc timeout. | Có thể xử lý hàng chục ngàn kết nối đồng thời với lượng CPU/RAM vô cùng khiêm tốn nhờ cơ chế I/O bất đồng bộ. |
| **Mức độ tiêu hao RAM** | Rất cao. Mỗi thread được cấp phát vùng nhớ stack riêng (thường là 1MB). 1000 threads tốn ít nhất 1GB RAM chỉ để duy trì trạng thái luồng. | Rất thấp. Do chỉ duy trì số lượng thread cố định (bằng số core CPU), RAM chủ yếu tiêu tốn cho việc giữ trạng thái session gọn nhẹ. |
| **Quản lý Backpressure** | Rất khó khăn hoặc không thể can thiệp ở tầng giao tiếp, dễ bị tình trạng tràn bộ đệm nếu Client không tiêu thụ kịp dữ liệu phát ra. | Hỗ trợ tuyệt đối cơ chế Backpressure từ Reactive Streams, cho phép điều tiết tốc độ phát token khớp với năng lực nhận của Client. |
| **Độ nhạy với mạng chập chờn** | Nếu Client ngắt kết nối đột ngột, Thread vẫn có thể bị block cho đến khi timeout hoạt động của IO hoàn tất. | Ngắt kết nối của Client được phát hiện lập tức thông qua sự kiện mạng, hủy luồng stream từ LLM ngay để tiết kiệm chi phí API. |

---

## PHẦN 4: MINH CHỨNG CHẠY THỰC TẾ (CONSOLE LOGS)

Dưới đây là dữ liệu log ghi nhận từ Console Spring Boot khi Client thực hiện gửi yêu cầu phân tích sự cố nghiêm trọng đến endpoint API `/api/v1/incident/stream?rawMessage=Phân tích sự cố nghẽn cảng Cát Lái ngày 15/10&temp=0.3&maxTokens=500`:

```text
2025-10-15 14:05:01.120  INFO 14205 --- [ctor-http-nio-2] c.e.d.c.IncidentStreamController        : Receiving stream request for incident analysis...
2025-10-15 14:05:01.125  INFO 14205 --- [ctor-http-nio-2] c.e.d.c.IncidentStreamController        : Applied Dynamic Options -> Temp: 0.3, MaxTokens: 500
2025-10-15 14:05:01.130  INFO 14205 --- [ctor-http-nio-2] c.e.d.c.IncidentStreamController        : Response Header 'X-Accel-Buffering: no' successfully injected.
2025-10-15 14:05:01.450 DEBUG 14205 --- [ctor-http-nio-2] o.s.ai.openai.client.OpenAiChatModel    : Sending request to OpenAI API streaming endpoint...

--- [SSE STREAM OUTPUT START] ---
data: "Dưới"
data: " đây"
data: " là"
data: " phân"
data: " tích"
data: " sơ"
data: " bộ"
data: " về"
data: " sự"
data: " cố"
data: " nghẽn"
data: " cảng"
data: " Cát"
data: " Lái:"
data: "\n"
data: "1."
data: " Nguyên"
data: " nhân:"
data: " Quá"
data: " tải"
data: " cục"
data: " bộ"
data: " do"
data: " tàu"
data: " cập"
data: " bến"
data: " cùng"
data: " khung"
data: " giờ."
--- [SSE STREAM OUTPUT END] ---

2025-10-15 14:05:05.890  INFO 14205 --- [ctor-http-nio-2] c.e.d.c.IncidentStreamController        : Streamed analysis completed for request. Total execution time: 4.77 seconds.
```

*Giao diện phía client nhận được dữ liệu dạng dòng (line-by-line) liên tục mà không bị đơ trình duyệt, đáp ứng hoàn hảo yêu cầu thời gian thực của Ban điều hành.*