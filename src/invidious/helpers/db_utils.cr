# Database utility functions with SQL injection protection
require "pg"

module Invidious::Database::Utils
  extend self

  # Validates that a PostgreSQL identifier (table name, view name, column name)
  # contains only safe characters to prevent SQL injection
  def validate_pg_identifier(name : String) : Bool
    # PostgreSQL identifiers must start with letter or underscore,
    # followed by letters, digits, underscores, or dollar signs
    # Maximum length is 63 characters
    return false if name.empty? || name.size > 63
    return false unless name.match(/^[a-zA-Z_][a-zA-Z0-9_$]*$/)

    # Additional check for reserved words could be added here
    true
  end

  # Safely quotes a PostgreSQL identifier to prevent SQL injection
  # This should be used for dynamic table/view/column names
  def quote_pg_identifier(name : String) : String
    raise ArgumentError.new("Invalid PostgreSQL identifier: #{name}") unless validate_pg_identifier(name)

    # Double quotes around identifier and escape any internal quotes
    %("#{name.gsub("\"", "\"\"")}")
  end

  # Safely constructs view name for user subscriptions
  def subscription_view_name(email : String) : String
    "subscriptions_#{sha256(email)}"
  end

  # Safely constructs legacy view name for user subscriptions
  def legacy_subscription_view_name(email : String) : String
    "subscriptions_#{sha256(email)[0..7]}"
  end

  # Execute SQL with a dynamically constructed table/view name safely
  def exec_with_identifier(db : DB::Database, sql_template : String, identifier : String)
    quoted_identifier = quote_pg_identifier(identifier)
    final_sql = sql_template % quoted_identifier
    db.exec(final_sql)
  end

  # Query with a dynamically constructed table/view name safely
  def query_with_identifier(db : DB::Database, sql_template : String, identifier : String, &)
    quoted_identifier = quote_pg_identifier(identifier)
    final_sql = sql_template % quoted_identifier
    db.query(final_sql) { |rs| yield rs }
  end

  # Query one with a dynamically constructed table/view name safely
  def query_one_with_identifier(db : DB::Database, sql_template : String, identifier : String, as type)
    quoted_identifier = quote_pg_identifier(identifier)
    final_sql = sql_template % quoted_identifier
    db.query_one(final_sql, as: type)
  end

  # Query one? with a dynamically constructed table/view name safely
  def query_one_with_identifier?(db : DB::Database, sql_template : String, identifier : String, as type)
    quoted_identifier = quote_pg_identifier(identifier)
    final_sql = sql_template % quoted_identifier
    db.query_one?(final_sql, as: type)
  end
end
