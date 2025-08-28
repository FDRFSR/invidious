class Invidious::Jobs::RefreshFeedsJob < Invidious::Jobs::BaseJob
  private getter db : DB::Database

  def initialize(@db)
  end

  def begin
    max_fibers = CONFIG.feed_threads
    active_fibers = 0
    active_channel = ::Channel(Bool).new

    loop do
      db.query("SELECT email FROM users WHERE feed_needs_update = true OR feed_needs_update IS NULL") do |rs|
        rs.each do
          email = rs.read(String)
          view_name = Invidious::Database::Utils.subscription_view_name(email)

          if active_fibers >= max_fibers
            if active_channel.receive
              active_fibers -= 1
            end
          end

          active_fibers += 1
          spawn do
            begin
              # Drop outdated views
              column_array = Invidious::Database.get_column_array(db, view_name)
              ChannelVideo.type_array.each_with_index do |name, i|
                if name != column_array[i]?
                  LOGGER.info("RefreshFeedsJob: DROP MATERIALIZED VIEW #{view_name}")
                  Invidious::Database::Utils.exec_with_identifier(db, "DROP MATERIALIZED VIEW %s", view_name)
                  raise "view does not exist"
                end
              end

              view_def = Invidious::Database::Utils.query_one_with_identifier(db, "SELECT pg_get_viewdef(%s)", view_name, as: String)
              if !view_def.includes? "WHERE ((cv.ucid = ANY (u.subscriptions))"
                LOGGER.info("RefreshFeedsJob: Materialized view #{view_name} is out-of-date, recreating...")
                Invidious::Database::Utils.exec_with_identifier(db, "DROP MATERIALIZED VIEW %s", view_name)
              end

              Invidious::Database::Utils.exec_with_identifier(db, "REFRESH MATERIALIZED VIEW %s", view_name)
              db.exec("UPDATE users SET feed_needs_update = false WHERE email = $1", email)
            rescue ex
              # Rename old views
              begin
                legacy_view_name = Invidious::Database::Utils.legacy_subscription_view_name(email)

                Invidious::Database::Utils.exec_with_identifier(db, "SELECT * FROM %s LIMIT 0", legacy_view_name)
                LOGGER.info("RefreshFeedsJob: RENAME MATERIALIZED VIEW #{legacy_view_name}")

                # For ALTER statements with two identifiers, we need to construct safely
                quoted_legacy = Invidious::Database::Utils.quote_pg_identifier(legacy_view_name)
                quoted_view = Invidious::Database::Utils.quote_pg_identifier(view_name)
                db.exec("ALTER MATERIALIZED VIEW #{quoted_legacy} RENAME TO #{quoted_view}")
              rescue ex
                begin
                  # While iterating through, we may have an email stored from a deleted account
                  if db.query_one?("SELECT true FROM users WHERE email = $1", email, as: Bool)
                    LOGGER.info("RefreshFeedsJob: CREATE #{view_name}")
                    quoted_view = Invidious::Database::Utils.quote_pg_identifier(view_name)
                    db.exec("CREATE MATERIALIZED VIEW #{quoted_view} AS #{MATERIALIZED_VIEW_SQL.call(email)}")
                    db.exec("UPDATE users SET feed_needs_update = false WHERE email = $1", email)
                  end
                rescue ex
                  LOGGER.error("RefreshFeedJobs: REFRESH #{email} : #{ex.message}")
                end
              end
            end

            active_channel.send(true)
          end
        end
      end

      sleep 5.seconds
      Fiber.yield
    end
  end
end
