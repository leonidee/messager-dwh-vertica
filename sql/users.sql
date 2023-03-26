COPY LEONIDGRISHENKOVYANDEXRU__STAGING.users (id, chat_name, registration_dt, country, age)
    FROM LOCAL '{path}'
    DELIMITER ','
    ENCLOSED '"';