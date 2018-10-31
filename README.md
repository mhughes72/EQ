# EQ

The code doesn't include any of the visualization steps yet (I had to move on to another commitment, will return to it after).

The solution itself is a bit scattered; I ended up using a number of different approaches (rdd, dataframes, sql).  I'd like to consolidate it a bit better when I have some more time.

SOME QUESTIONS:
- The POIList.csv file provided has 2 POIIDs with identical geo-location data.  I've left them both in for now.  The code will currently defer to POID1.
- The first instruction under 'Cleanup' isn't clear to me.  I ran some queries and didn't find any timestamps that match up with Longitude or Latitude data.  I assume I'm looking at the wrong data?
- Larger implementations are all possible (Hive, etc...).  I'd need more details about your infrastructure to comment properly.

NOTES
- Distance is in Meters.

TO DO
- Visualization steps
- Further expansion analysis
- Test script on Matt's AWS account.
