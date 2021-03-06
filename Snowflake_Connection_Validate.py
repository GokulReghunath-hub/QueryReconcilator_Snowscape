#!/usr/bin/env python
import snowflake.connector
import Config

# Gets the version
ctx = snowflake.connector.connect(
    user=Config.user,
    authenticator='externalbrowser',
    account='****'
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()
