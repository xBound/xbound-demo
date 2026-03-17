[demo-sketch.jpeg](sketch/demo-sketch.jpeg) 

Ok, so I want to create a demo for VLDB26. The sketch is in the figure.

First, it should have a main dashboard where:

(i) a SQL input on the left (see figure; I have a cool style for it later). It has square buttons on the left with: (a) run, (b) plan view, (c) leaderboard.

(ii) on the right there are fields to select: benchmark, query. Now, we don't select the system. When the user selects the query, it should apper in the SQL input.

(iii) on the bottom part, we should see a plot with the systems (duckdb, postgres, fabric dw) and box plots. These will be generated from JSON => you need to compute sth. The JSON's will come. just simulate them for now.

The plot should have the option to put the `xBound-ed system`, where a new box will appear to the right of the system.

When the user clicks the plan view, we should show for a selected system the query plan from the system. That will also be provided with a JSON. your task is then to to visualize the plan like as a tree.

+=====

The demo should be written in electron with simple javacsript. no react.

====

Another tab