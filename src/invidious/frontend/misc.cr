module Invidious::Frontend::Misc
  extend self

  def redirect_url(env : HTTP::Server::Context)
    prefs = env.get("preferences").as(Preferences)

    if prefs.automatic_instance_redirect
      current_page = env.get?("current_page").as(String)
      return "/redirect?referer=#{current_page}"
    else
      # Validate that the resource starts with / to prevent open redirect
      resource = env.request.resource
      if resource.starts_with?("/")
        return "https://redirect.invidious.io#{resource}"
      else
        # If resource doesn't start with /, use safe default
        return "https://redirect.invidious.io/"
      end
    end
  end
end
