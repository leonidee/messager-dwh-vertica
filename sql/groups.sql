COPY LEONIDGRISHENKOVYANDEXRU__STAGING.groups (id, admin_id, group_name, registration_dt, is_private)
    FROM LOCAL '{path}'
    DELIMITER ','
    ENCLOSED '"';