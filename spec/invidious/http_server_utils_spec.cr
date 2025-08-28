require "../spec_helper"

Spectator.describe "Invidious::HttpServer::Utils" do
  describe "proxy_video_url" do
    it "handles valid URLs correctly" do
      result = Invidious::HttpServer::Utils.proxy_video_url("https://example.com/path?param=value")
      expect(result).to contain("host=example.com")
      expect(result).to contain("/path?param=value")
    end

    it "handles URLs with nil host gracefully" do
      # These URLs would previously cause NilAssertionError
      empty_url = Invidious::HttpServer::Utils.proxy_video_url("")
      expect(empty_url).to eq("")

      relative_url = Invidious::HttpServer::Utils.proxy_video_url("relative/path")
      expect(relative_url).to eq("relative/path")

      absolute_path = Invidious::HttpServer::Utils.proxy_video_url("/absolute/path")
      expect(absolute_path).to eq("/absolute/path")

      invalid_url = Invidious::HttpServer::Utils.proxy_video_url("invalid-url")
      expect(invalid_url).to eq("invalid-url")
    end

    it "adds region parameter when provided" do
      result = Invidious::HttpServer::Utils.proxy_video_url("https://example.com/path", region: "US")
      expect(result).to contain("region=US")
      expect(result).to contain("host=example.com")
    end

    it "preserves existing query parameters" do
      result = Invidious::HttpServer::Utils.proxy_video_url("https://example.com/path?existing=param")
      expect(result).to contain("existing=param")
      expect(result).to contain("host=example.com")
    end
  end
end