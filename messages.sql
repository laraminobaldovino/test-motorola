DECLARE min_date DATE DEFAULT DATE("2024-09-14");
DECLARE max_date DATE DEFAULT CURRENT_DATE();

-- comment


CREATE OR REPLACE TABLE `demobleett.ia_fravega_ac_logs.rag_times_IA` AS
(

WITH tiempos_ac_ia AS(
SELECT #MIN(DATE(timestamp)),
       #MAX(DATE(timestamp))
       *,
       ROW_NUMBER() OVER (PARTITION BY session_id_rag ORDER BY timestamp) AS row_number,
       CONCAT(session_id_rag, '-', CAST(timestamp AS STRING)) AS created_msg_id
       FROM `demobleett.ia_fravega_ac_logs.ia_fravega_rag_logs`
WHERE TRUE
      AND DATE(timestamp) BETWEEN min_date AND max_date
),

tiempos_BM AS (
SELECT #MIN(DATE(timestamp)),
       #MAX(DATE(timestamp))
       id,
       message,
       creation_time,
       creation_dow_name,
       creation_hour,
       message_number_botmaker,
       comprehension_type,
       comprehension_type_2,
       msg_from,
       previous_msg_from,
       sent_by_bot,
       sent_by_user,
       sent_by_operator,
       original_bot_message,
       original_user_message,
       previous_message,
       chat_platform_id_normalized,
       session_id,
       previous_session_id,
       session_id_botmaker,
       session_creation_time_botmaker,
       session_duration,
       session_creation_month,
       customer_id
       FROM `demobleett.demo_bleett.demo_bleett_metrics_tbl`
WHERE TRUE 
      AND DATE(timestamp) BETWEEN min_date AND max_date
)
SELECT *
FROM tiempos_ac_ia
LEFT JOIN tiempos_BM
ON tiempos_ac_ia.messageId = tiempos_BM.id
WHERE TRUE
ORDER BY session_bm ASC,
         timestamp ASC

)
