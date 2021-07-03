
class Database:

    def __init__(self, client) -> None:
        self.client = client

    def PartOne(self):
        """
        Show top 10 time slots (year and month) with the highest trade value (i.e. import value + export value).
        Note: Show “time_ref” and “trade value” in your results. 
        """


        query_job = self.client.query(
        """
        SELECT time_ref, SUM(value) AS tradeValue
        FROM task2.gsquarterlySeptember20
        GROUP BY time_ref
        ORDER BY SUM(value) DESC
        LIMIT 10
        """
        )
        sqlResults = query_job.result()
        results = []

        for sqlResult in sqlResults:
            print(sqlResult)
            results.append({"time_ref": sqlResult.time_ref, "tradeValue": sqlResult.tradeValue})

        return results

    def PartTwo(self):
        """
        Show top 50 countries with the highest total trade deficit value (i.e. import value - export value) of goods from 2014 to 2016 where status is “F”.
        Note: Show “country_label” (full country name), “product_type”, “trade deficit value” (in a descending order), and “status” in your result
        """

        query_job = self.client.query(
        """
        WITH country_trade AS (SELECT cc.country_code, cc.country_label, qs.product_type, qs.status
                    FROM `cc-ass1-bigquery.task2.country_classification` cc INNER JOIN `cc-ass1-bigquery.task2.gsquarterlySeptember20` qs ON cc.country_code = qs.country_code
                    WHERE qs.status = 'F'),

        exports AS (SELECT qs.country_code,
                    SUM (qs.value) AS export_total
                    FROM `cc-ass1-bigquery.task2.gsquarterlySeptember20` qs
                    WHERE qs.account = 'Exports' 
                        AND qs.country_code <> 'TOT'
                    GROUP BY qs.country_code
                    ORDER BY export_total DESC),

        imports AS (SELECT qs.country_code,
                    SUM (qs.value) AS import_total
                    FROM `cc-ass1-bigquery.task2.gsquarterlySeptember20` qs
                    WHERE qs.account = 'Imports'
                    GROUP BY qs.country_code
                    ORDER BY import_total DESC),

        trade_total AS (SELECT imports.country_code, imports.import_total - exports.export_total AS deficit
                    FROM exports INNER JOIN imports ON imports.country_code = exports.country_code)

        SELECT ct.country_label, ct.product_type, ct.status, tt.deficit
        FROM country_trade ct INNER JOIN trade_total tt ON ct.country_code = tt.country_code
        GROUP BY ct.country_label, ct.product_type, ct.status, tt.deficit
        ORDER BY tt.deficit DESC
        LIMIT 50
        """)

        sqlResults = query_job.result()
        results = []

        for sqlResult in sqlResults:
            results.append({"country_label": sqlResult.country_label, "product_type": sqlResult.product_type, "status": sqlResult.status, "deficit": sqlResult.deficit})

        return results

    def PartThree(self):
        pass