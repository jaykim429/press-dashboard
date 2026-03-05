import re
from collections import defaultdict

path = r'c:\Users\admin1\Documents\보도자료 테스트\local_dashboard.py'
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

NEW_METHOD = '''
    def handle_notifications(self, qs):
        """Return today's new articles grouped by organization and type."""
        since_date = (qs.get("since", [""])[0] or "").strip()
        conn = self._db()
        try:
            date_expr = "date(replace(substr(published_at, 1, 10), \'.\', \'-\'))"
            if since_date:
                where = f"{date_expr} > date(?)"
                params = [since_date]
            else:
                where = f"{date_expr} = date(\'now\', \'localtime\')"
                params = []

            type_case = """
                CASE
                    WHEN source_channel IN (\'fss_press_explainer\',\'fsc_press_explainer\') THEN \'보도설명자료\'
                    WHEN source_channel IN (\'fsc_admin_guidance_notice\',\'fss_admin_guidance_notice\') THEN \'행정지도 예고\'
                    WHEN source_channel IN (\'fsc_admin_guidance_enforcement\',\'fss_admin_guidance_enforcement\') THEN \'행정지도 시행\'
                    WHEN source_channel = \'fsc_law_interpretation\' THEN \'법령해석\'
                    WHEN source_channel = \'fsc_no_action_opinion\' THEN \'비조치의견서\'
                    WHEN source_channel IN (\'fsc_rule_change_notice\',\'ksd_rule_change_notice\',\'krx_rule_change_notice\',\'kofia_rule_change_notice\') THEN \'규정 제개정 예고\'
                    WHEN source_channel IN (\'fsc_regulation_notice\',\'krx_recent_rule_change\',\'kofia_recent_rule_change\') THEN \'최신 제·개정 정보\'
                    WHEN source_channel IN (\'kfb_publicdata_other\',\'fsec_bbs_222\') THEN \'기타자료\'
                    ELSE \'보도자료\'
                END
            """

            total = conn.execute(f"SELECT COUNT(*) FROM articles WHERE {where}", params).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT
                    COALESCE(organization, \'(기관 없음)\') AS org,
                    {type_case} AS type_label,
                    COUNT(*) AS cnt
                FROM articles
                WHERE {where}
                GROUP BY org, type_label
                ORDER BY cnt DESC, org ASC
                """,
                params,
            ).fetchall()

            from collections import defaultdict
            grouped = defaultdict(list)
            for row in rows:
                grouped[row["org"]].append({"type": row["type_label"], "count": row["cnt"]})
            result = [
                {"organization": org, "items": items}
                for org, items in sorted(grouped.items(), key=lambda x: -sum(i["count"] for i in x[1]))
            ]
            self._json_response({"total": total, "groups": result})
        finally:
            conn.close()

'''

# Insert before _get_kiwi classmethod
marker = '    @classmethod\n    def _get_kiwi(cls):'
if marker in content:
    content = content.replace(marker, NEW_METHOD + marker, 1)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print('Done - inserted handle_notifications')
else:
    # try CRLF variant
    marker2 = '    @classmethod\r\n    def _get_kiwi(cls):'
    if marker2 in content:
        content = content.replace(marker2, NEW_METHOD + marker2, 1)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        print('Done (CRLF) - inserted handle_notifications')
    else:
        print('ERROR: marker not found')
        idx = content.find('_get_kiwi')
        print(repr(content[max(0,idx-80):idx+40]))
