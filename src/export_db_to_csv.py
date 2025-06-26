import pandas as pd
import sqlalchemy
import os
import re

db_url = os.environ.get("DB_URL")
engine = sqlalchemy.create_engine(db_url if db_url else "")
query = """
SELECT
   passenger_email,
   COUNT(ticket_id) AS total_tickets,
   AVG(ticket_amount_usd) AS avg_spent,
   AVG(DATEDIFF(departure_time, purchased_at)) AS days_in_advance_avg,
   AVG(has_discount) AS discount_rate,
   AVG(CASE WHEN order_channel = 'Web' THEN 1 ELSE 0 END) AS web_rate,
   DATEDIFF(CURDATE(), MAX(purchased_at)) AS last_purchase_days_ago
FROM v_tickets_for_ml
WHERE departure_time IS NOT NULL
  AND purchased_at IS NOT NULL
  AND DATEDIFF(departure_time, purchased_at) BETWEEN 0 AND 120
GROUP BY passenger_email
HAVING COUNT(ticket_id) >= 2 AND COUNT(ticket_id) <= 50
ORDER BY total_tickets DESC
"""
df = pd.read_sql(query, engine)

# Anonymize email using a lambda function
df['passenger_email'] = df['passenger_email'].apply(
    lambda email: (
      email[0] +
      re.sub(r'[A-Za-z0-9]', 'x', email.split('@', 1)[0][1:]) +
      '@' + email.split('@', 1)[1]
    )
)

df.to_csv("output.csv", index=False)
