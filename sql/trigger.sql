
CREATE TABLE IF NOT EXISTS user_log_before (
    user_id BIGINT ,
    login VARCHAR(255) NOT NULL,
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS user_log_after (
    user_id BIGINT ,
    login VARCHAR(255) NOT NULL,
    gravatar_id VARCHAR(255),
    avatar_url VARCHAR(255),
    url VARCHAR(255),
    state VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) PRIMARY KEY
);

DELIMITER //

CREATE TRIGGER before_update_users
BEFORE UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, avatar_url, url,state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "UPDATE");
END//

CREATE TRIGGER before_insert_users
BEFORE INSERT ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, avatar_url, url,state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "INSERT");
END//

CREATE TRIGGER before_delete_users
BEFORE DELETE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, avatar_url, url,state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "DELETE");
END//

CREATE TRIGGER after_update_users
AFTER UPDATE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id, login, gravatar_id, avatar_url, url,state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "UPDATE");
END//

CREATE TRIGGER after_insert_users
AFTER INSERT ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id, login, gravatar_id, avatar_url, url,state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.avatar_url, NEW.url, "INSERT");
END//

CREATE TRIGGER after_delete_users
AFTER DELETE ON Users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id, login, gravatar_id, avatar_url, url,state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.avatar_url, OLD.url, "DELETE");
END//

DELIMITER ;

INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (1, 'alice', 'g123a', 'https://avatar.com/alice.png', 'https://example.com/alice');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (2, 'bob', 'g456b', 'https://avatar.com/bob.png', 'https://example.com/bob');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (3, 'carol', 'g789c', 'https://avatar.com/carol.png', 'https://example.com/carol');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (4, 'dave', 'g012d', 'https://avatar.com/dave.png', 'https://example.com/dave');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (5, 'eve', 'g345e', 'https://avatar.com/eve.png', 'https://example.com/eve');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (6, 'frank', 'g678f', 'https://avatar.com/frank.png', 'https://example.com/frank');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (7, 'grace', 'g901g', 'https://avatar.com/grace.png', 'https://example.com/grace');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (8, 'heidi', 'g234h', 'https://avatar.com/heidi.png', 'https://example.com/heidi');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (9, 'ivan', 'g567i', 'https://avatar.com/ivan.png', 'https://example.com/ivan');
INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (10, 'judy', 'g890j', 'https://avatar.com/judy.png', 'https://example.com/judy');

UPDATE Users SET login = 'alice_updated' WHERE user_id = 1;
UPDATE Users SET avatar_url = 'https://new.avatar.com/bob.png' WHERE user_id = 2;
UPDATE Users SET url = 'https://newsite.com/carol' WHERE user_id = 3;
UPDATE Users SET gravatar_id = 'gx012d' WHERE user_id = 4;
UPDATE Users SET login = 'eve_new', url = 'https://eve.updated.com' WHERE user_id = 5;
UPDATE Users SET avatar_url = NULL WHERE user_id = 6;
UPDATE Users SET gravatar_id = 'gx901g' WHERE user_id = 7;
UPDATE Users SET login = 'heidi_2' WHERE user_id = 8;
UPDATE Users SET avatar_url = 'https://cdn.avatar.com/ivan.png' WHERE user_id = 9;
UPDATE Users SET url = 'https://judy.blog.com' WHERE user_id = 10;

DELETE FROM Users WHERE user_id = 1;
DELETE FROM Users WHERE user_id = 2;
DELETE FROM Users WHERE user_id = 3;
DELETE FROM Users WHERE user_id = 4;
