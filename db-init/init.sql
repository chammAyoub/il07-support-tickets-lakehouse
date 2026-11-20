CREATE TABLE gold_tickets (
    ticket_id         INT PRIMARY KEY,
    category          TEXT,
    priority_pred     TEXT,
    status            TEXT,
    created_at        TIMESTAMP,
    resolution_hours  NUMERIC,
    sla_target_hours  INT
);

INSERT INTO gold_tickets
SELECT i, cat, prio, st,
       now() - random() * interval '30 days',
       CASE WHEN st IN ('RESOLVED','CLOSED')
            THEN round((random()*48)::numeric, 1) END,
       24
FROM (
    SELECT i,
      (ARRAY['facturation','annulation','livraison','compte','support'])[1+floor(random()*5)::int] AS cat,
      (ARRAY['LOW','MEDIUM','HIGH'])[1+floor(random()*3)::int] AS prio,
      (ARRAY['OPEN','IN_PROGRESS','RESOLVED','CLOSED'])[1+floor(random()*4)::int] AS st
    FROM generate_series(1,500) AS i
) t;
