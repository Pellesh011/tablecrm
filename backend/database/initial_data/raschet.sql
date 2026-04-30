CREATE
OR REPLACE FUNCTION public.rachet(token text, today integer)
 RETURNS TABLE(pb_id integer, balance numeric, pr_id integer, incoming numeric, outgoing numeric)
 LANGUAGE plpgsql
AS $function$

BEGIN

CREATE
TEMP TABLE pays -- стоит использовать наиболее уникальное имя

ON COMMIT DROP -- удаляем таблицу при завершении транзакции

AS



SELECT payments.type,

       CASE WHEN payments.type = 'transfer' THEN true ELSE false END AS is_transfer,

       payments.amount,

       payments.date,

       payments.parent_id,

       payments.paybox,

       payments.paybox_to,

       0                                                             AS paybox_date,

       payments.project_id
FROM payments
WHERE payments.status IS TRUE
  AND payments.is_deleted IS FALSE
  AND payments.is_deleted IS FALSE
  AND payments.paybox IN (SELECT payboxes.id
                          FROM payboxes
                          WHERE payboxes.cashbox IN (SELECT relation_tg_cashboxes.cashbox_id
                                                     FROM relation_tg_cashboxes

                                                     WHERE relation_tg_cashboxes.token = $1));


------------ Манипуляции с таблицей: ------------------


INSERT INTO pays

    (type, amount, date, paybox, is_transfer, paybox_date)

SELECT 'incoming',
       amount, date, paybox_to, true, paybox_date

FROM pays

WHERE type = 'transfer';



UPDATE
    pays

SET paybox_date = (SELECT CAST(payboxes.balance_date AS INT) FROM payboxes WHERE payboxes.id = pays.paybox)

WHERE paybox_date = 0;



UPDATE pays
SET type = 'outgoing'
WHERE type = 'transfer';

UPDATE pays
SET amount = amount * -1
WHERE type = 'outgoing';



RETURN QUERY
       (SELECT a.pb_id, a.balance, b.pr_id, b.incoming, b.outgoing FROM (



            SELECT *, row_number() over() AS rn FROM (

                SELECT

                    paybox AS pb_id,

                    ROUND(sum(amount)::numeric, 2) AS balance

                FROM pays

                WHERE date >= paybox_date AND date <= $2 AND parent_id is NULL

                GROUP BY paybox)

                AS payboxes)

            AS a FULL OUTER JOIN



            (SELECT *, row_number() over() AS rn FROM (

                SELECT

                    project_id AS pr_id,

                    ROUND(sum(CASE WHEN type = 'incoming' AND is_transfer IS FALSE THEN amount ELSE 0 END)::numeric, 2) AS incoming,

                    ROUND(sum(CASE WHEN type = 'outgoing' AND is_transfer IS FALSE THEN amount * -1 ELSE 0 END)::numeric, 2) AS outgoing

                    FROM pays

                    WHERE project_id IS NOT NULL

                    GROUP BY project_id)

                    AS projects)

            AS b ON a.rn = b.rn);



END

$function$
