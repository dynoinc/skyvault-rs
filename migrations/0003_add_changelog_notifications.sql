-- Create a function to notify on changelog changes
CREATE OR REPLACE FUNCTION notify_changelog_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Send notification with the new changelog entry ID
    PERFORM pg_notify('changelog_update', NEW.id::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to call the function on INSERT
CREATE TRIGGER changelog_notify_trigger
    AFTER INSERT ON changelog
    FOR EACH ROW
    EXECUTE FUNCTION notify_changelog_change(); 