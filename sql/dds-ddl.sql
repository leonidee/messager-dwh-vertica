/*
 DWH DDL
 */
-- drop hubs
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.h_users CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.h_dialogs CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.h_groups CASCADE;

-- drop links
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_user_message CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_admins CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.l_groups_dialogs CASCADE;

-- drop sattelite
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.s_admins CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.s_user_socdem CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.s_user_chatinfo CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.s_dialog_info CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.s_group_name CASCADE;
DROP TABLE IF EXISTS LEONIDGRISHENKOVYANDEXRU__DWH.s_group_private_status CASCADE;

/*
 HUBS
*/
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.h_users
(
    hk_user_id      bigint PRIMARY KEY,
    user_id         int,
    registration_dt datetime,
    load_dt         datetime,
    load_src        varchar(20)
)
    ORDER BY load_dt
    SEGMENTED BY hk_user_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.h_dialogs
(
    hk_message_id bigint PRIMARY KEY,
    message_id    int,
    message_ts    datetime,
    load_dt       datetime,
    load_src      varchar(20)
)
    ORDER BY load_dt
    SEGMENTED BY hk_message_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.h_groups
(
    hk_group_id     bigint PRIMARY KEY,
    group_id        int,
    registration_dt datetime,
    load_dt         datetime,
    load_src        varchar(20)
)
    ORDER BY load_dt
    SEGMENTED BY hk_group_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

/*
 LINKS
*/

CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_user_message
(
    hk_l_user_message bigint PRIMARY KEY,
    hk_user_id        bigint NOT NULL
        CONSTRAINT fk_l_user_message_user REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_users (hk_user_id),
    hk_message_id     bigint NOT NULL
        CONSTRAINT fk_l_user_message_message REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_dialogs (hk_message_id),
    load_dt           datetime,
    load_src          varchar(20),

    FOREIGN KEY (hk_user_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_users (hk_user_id),
    FOREIGN KEY (hk_message_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_dialogs (hk_message_id)
)
    ORDER BY load_dt
    SEGMENTED BY hk_user_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_admins
(
    hk_l_admin_id bigint PRIMARY KEY,
    hk_user_id    bigint NOT NULL,
    hk_group_id   bigint NOT NULL,
    load_dt       datetime,
    load_src      varchar(20),
    FOREIGN KEY (hk_user_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_users (hk_user_id),
    FOREIGN KEY (hk_group_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_groups (hk_group_id)
)
    ORDER BY load_dt
    SEGMENTED BY hk_l_admin_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs bigint PRIMARY KEY,
    hk_message_id       bigint NOT NULL,
    hk_group_id         bigint NOT NULL,
    load_dt             datetime,
    load_src            varchar(20),
    FOREIGN KEY (hk_message_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_dialogs (hk_message_id),
    FOREIGN KEY (hk_group_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_groups (hk_group_id)
)
    ORDER BY load_dt
    SEGMENTED BY hk_l_groups_dialogs ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

/*
 SATELLITES
*/
-- admins
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.s_admins
(
    hk_admin_id bigint NOT NULL,
    is_admin    boolean,
    admin_from  datetime,
    load_dt     datetime,
    load_src    varchar(20),

    FOREIGN KEY (hk_admin_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.l_admins (hk_l_admin_id)
)
    ORDER BY load_dt
    SEGMENTED BY hk_admin_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- users socdem
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.s_user_socdem
(
    hk_user_id bigint NOT NULL,
    country    varchar(100),
    age        int,
    load_dt    datetime,
    load_src   varchar(20),

    FOREIGN KEY (hk_user_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_users (hk_user_id)
)
    ORDER BY load_dt
    SEGMENTED BY hk_user_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--users chat info
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.s_user_chatinfo
(
    hk_user_id bigint NOT NULL,
    chat_name  varchar(1000),
    load_dt    timestamp,
    load_src   varchar(20),

    FOREIGN KEY (hk_user_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_users (hk_user_id)
)
    ORDER BY load_dt
    SEGMENTED BY hk_user_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- dialog info
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.s_dialog_info
(
    hk_message_id bigint NOT NULL,
    message       varchar(1000),
    message_from  int,
    message_to    int,
    load_dt       datetime,
    load_src      varchar(20),

    FOREIGN KEY (hk_message_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_dialogs (hk_message_id),
    FOREIGN KEY (message_from) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_users (hk_user_id),
    FOREIGN KEY (message_to) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_users (hk_user_id)
)
    ORDER BY load_dt
    SEGMENTED BY hk_message_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

-- s_group_name
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.s_group_name
(
    hk_group_id bigint NOT NULL,
    group_name  varchar(100),
    load_dt     datetime,
    load_src    varchar(20),

    FOREIGN KEY (hk_group_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_groups (hk_group_id)
)
    ORDER BY load_dt
    SEGMENTED BY hk_group_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--group private status
CREATE TABLE LEONIDGRISHENKOVYANDEXRU__DWH.s_group_private_status
(
    hk_group_id bigint NOT NULL,
    is_private  boolean,
    load_dt     datetime,
    load_src    varchar(20),

    FOREIGN KEY (hk_group_id) REFERENCES LEONIDGRISHENKOVYANDEXRU__DWH.h_groups (hk_group_id)
)
    ORDER BY load_dt
    SEGMENTED BY hk_group_id ALL NODES
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);