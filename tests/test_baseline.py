"""Baseline đầu vào/đầu ra, chạy hoàn toàn offline bằng unittest."""

import logging
import socket
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from offline_support import load_targets


class OfflineCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Chặn socket cả khi import để accidental network không lọt ra ngoài.
        cls.network = patch.object(socket.socket, "connect", side_effect=AssertionError("Network blocked"))
        cls.network.start()
        cls.addClassCleanup(cls.network.stop)
        cls.a = load_targets()


class SkillRulesTests(OfflineCase):
    def test_skill_aliases_whitespace_case_and_unknown(self):
        for raw, expected in [
            (" K8S ", "kubernetes"), ("Golang", "go"),
            ("React Native", "react_native"), ("NodeJS", "node.js"),
            (" NewTool ", "newtool"), ("  ", ""),
        ]:
            with self.subTest(raw=raw):
                self.assertEqual(self.a.gap._normalize_skill(raw), expected)

    def test_skill_matching_tokens_avoid_substrings(self):
        for market, skills, expected in [
            ("Spring", ["Spring Boot"], True), ("API", ["RESTful API"], True),
            ("Java", ["JavaScript"], False), ("SQL", ["MySQL"], False),
            ("React", ["React.js"], True), ("Python", [], False), ("", ["Python"], False),
        ]:
            with self.subTest(market=market, skills=skills):
                self.assertEqual(self.a.gap._skill_in_cv(market, skills), expected)

    def test_existing_c_family_token_collision(self):
        # Characterization: hiện code coi C và C++ cùng token, trái docstring.
        self.assertTrue(self.a.gap._skill_in_cv("C", ["C++"]))

    def test_role_category_prefers_specific_keyword(self):
        for role, expected in [
            ("Senior Product Manager", "Product & Business Analysis"),
            ("Data Engineer", "Data Engineering"),
            ("BACK-END Developer", "Backend Development"), ("", None),
            ("Unlisted Role", None),
        ]:
            with self.subTest(role=role):
                self.assertEqual(self.a.gap._infer_category_from_keywords(role), expected)

    def test_matched_skills_respect_boundaries_and_symbols(self):
        self.assertEqual(
            self.a.matcher._find_matched_skills(
                ["Java", "JavaScript", "C", "C++", "C#", ".NET", "SQL"],
                "JavaScript, C++, C# and .NET with MySQL",
            ), ["JavaScript", "C++", "C#", ".NET"],
        )

    def test_matched_skills_preserve_order_cap_eight_and_empty(self):
        skills = [f"Skill{i}" for i in range(10)]
        self.assertEqual(self.a.matcher._find_matched_skills(skills, ", ".join(skills)), skills[:8])
        self.assertEqual(self.a.matcher._find_matched_skills([], "Python"), [])


class ClassificationTests(OfflineCase):
    def classifier(self, text="search_job", error=None):
        client = Mock()
        client.messages.create.side_effect = error or None
        client.messages.create.return_value = SimpleNamespace(content=[SimpleNamespace(text=text)])
        with patch.object(self.a.anthropic, "Anthropic", return_value=client):
            return self.a.classifier.QueryClassifier()

    def test_greetings_and_short_off_topic(self):
        c = self.classifier(error=AssertionError("LLM not allowed on fast path"))
        for text in ["  xin chào!  ", "HELLO", "thanks", "weather today", ""]:
            with self.subTest(text=text):
                self.assertEqual(c.classify(text), self.a.schemas.QueryType.out_of_scope)

    def test_chart_fast_path(self):
        c = self.classifier(error=AssertionError("LLM not allowed on chart path"))
        self.assertEqual(c.classify("vẽ biểu đồ số lượng job"), self.a.schemas.QueryType.analytics)

    def test_it_keyword_and_eight_word_boundary(self):
        f = self.a.classifier.is_short_off_topic
        self.assertFalse(f("Python jobs"))
        self.assertTrue(f("one two three four five six seven"))
        self.assertFalse(f("one two three four five six seven eight"))

    def test_llm_label_normalization(self):
        self.assertEqual(self.classifier("  SEARCH_JOB\n").classify("Python developer jobs"), self.a.schemas.QueryType.search_job)

    def test_unknown_label_falls_back(self):
        self.assertEqual(self.classifier("not-a-category").classify("Python jobs"), self.a.schemas.QueryType.career_advice)

    def test_llm_timeout_falls_back(self):
        self.assertEqual(self.classifier(error=TimeoutError("fixture timeout")).classify("Python jobs"), self.a.schemas.QueryType.career_advice)

    def test_existing_forecast_label_missing_in_enum(self):
        # Prompt có forecast nhưng QueryType chưa có: ghi nhận, không sửa nghiệp vụ.
        self.assertEqual(self.classifier("forecast").classify("forecast Python jobs"), self.a.schemas.QueryType.career_advice)


class CVResultTests(OfflineCase):
    def job(self, ident, title, text, score):
        return self.a.schemas.JobResult(
            job_id=ident, job_title=title, job_link=f"https://example.invalid/jobs/{ident}",
            text_content=text, score=score,
        )

    def test_soft_sort_is_stable_and_does_not_discard(self):
        jobs = [self.job(1, "Dev", "Ha Noi", 4), self.job(2, "Dev", "HCM", 3),
                self.job(3, "Dev", "Da Nang", 2), self.job(4, "Dev", "Ho Chi Minh", 1)]
        ordered = self.a.matcher._soft_sort(jobs, ["hcm"], None)
        self.assertEqual([j.job_id for j in ordered], [2, 4, 1, 3])
        self.assertEqual([j.job_id for j in jobs], [1, 2, 3, 4])
        self.assertEqual(self.a.matcher._soft_sort(jobs, [], None), jobs)

    def test_preference_location_or_mode_and_unknown_location(self):
        f = self.a.matcher._matches_preference
        self.assertTrue(f("Remote job in Ha Noi", ["hcm"], "remote"))
        self.assertTrue(f("Office in TestCity", ["testcity"], None))
        self.assertFalse(f("Office in Ha Noi", ["hcm"], "remote"))

    def test_level_penalty_known_unknown_and_overqualified(self):
        for level, title, expected in [
            ("junior", "Senior Developer", .72), ("intern", "Tech Lead", .40),
            ("senior", "Junior Developer", 1), (None, "Senior Dev", 1),
            ("unknown", "Senior Dev", 1), ("junior", "Developer", 1),
        ]:
            with self.subTest(level=level, title=title):
                self.assertEqual(self.a.matcher._level_penalty(level, title), expected)

    def test_score_conversion_and_label_thresholds(self):
        for logit, expected in [(-2, 11.9), (0, 50.0), (2, 88.1)]:
            self.assertEqual(self.a.matcher._sigmoid_norm(logit), expected)
        for score, expected in [(0, "Fair"), (49.9, "Fair"), (50, "Good"),
                                (69.9, "Good"), (70, "Very Good"), (84.9, "Very Good"), (85, "Excellent")]:
            self.assertEqual(self.a.matcher._label(score), expected)

    def test_existing_extreme_negative_logit_overflow(self):
        with self.assertRaises(OverflowError):
            self.a.matcher._sigmoid_norm(-1000)

    def test_match_returns_annotated_results_and_preserves_source(self):
        jobs = [self.job(1, "Developer", "Python in Ha Noi", 2),
                self.job(2, "Senior Developer", "Python, SQL in Ho Chi Minh", 0)]
        search = Mock()
        search.search_multi.return_value = jobs
        profile = self.a.schemas.CVProfile(
            skills=["Python", "SQL"], level="junior", preferred_locations=["hcm"], search_queries=["Python jobs"],
        )
        result = self.a.matcher.CVMatcherService(search).match(profile, top_k=2)
        self.assertEqual([r.job_id for r in result], [2, 1])
        self.assertEqual([r.match_score for r in result], [36.0, 88.1])
        self.assertEqual(result[0].matched_skills, ["Python", "SQL"])
        self.assertEqual(result[0].match_label, "Fair")
        self.assertEqual(result[0].raw_score, 0)
        self.assertEqual([j.job_id for j in jobs], [1, 2])
        # Hiện match giữ ưu tiên vị trí, không sort lại theo adjusted score.
        self.assertEqual(len(self.a.matcher.CVMatcherService(search).match(profile, top_k=1)), 1)

    def test_match_empty_results_and_missing_queries(self):
        search = Mock()
        search.search_multi.return_value = []
        matcher = self.a.matcher.CVMatcherService(search)
        self.assertEqual(matcher.match(self.a.schemas.CVProfile(search_queries=["Python"])), [])
        with self.assertRaises(ValueError):
            matcher.match(self.a.schemas.CVProfile())


class SkillGapResultTests(OfflineCase):
    def connection(self, result_sets):
        cursor = Mock()
        # Fake DB-API cursor cung cấp schema và rows, để _run thật xử lý.
        batches = iter(result_sets)
        def execute(_sql):
            columns, rows = next(batches)
            cursor.description = [(col,) for col in columns]
            cursor.fetchall.return_value = rows
        cursor.execute.side_effect = execute
        conn = Mock()
        conn.cursor.return_value = cursor
        return conn

    def test_analysis_computes_frequency_coverage_present_missing(self):
        conn = self.connection([
            (["total"], [(10,)]),
            (["skill_name", "job_count", "skill_group"], [("PYTHON", 8, "Language"), ("SQL", 5, None), ("DOCKER", 3, "Tool")]),
        ])
        result = self.a.gap.SkillGapAnalyzerService()._analyze_one(conn, "Data Engineering", ["Python", "SQL"])
        self.assertEqual(result.total_jobs_analyzed, 10)
        self.assertEqual(result.present_skills, ["PYTHON", "SQL"])
        self.assertEqual(result.cv_coverage_score, 66.7)
        self.assertEqual([(s.skill_name, s.market_frequency) for s in result.missing_skills], [("DOCKER", 30.0)])
        self.assertEqual(result.all_market_skills[1].skill_group, "Other")

    def test_analysis_empty_totals_or_no_skills(self):
        for batches in [([(["total"], [])]), ([(["total"], [(0,)])]),
                        ([(["total"], [(5,)]), (["skill_name", "job_count"], [])])]:
            with self.subTest(batches=batches):
                self.assertIsNone(self.a.gap.SkillGapAnalyzerService()._analyze_one(self.connection(batches), "Backend", []))

    def test_analysis_caps_display_and_uses_top_fifteen_coverage(self):
        rows = [(f"SKILL{i}", 1) for i in range(20)]
        conn = self.connection([(["total"], [(10,)]), (["skill_name", "job_count"], rows)])
        result = self.a.gap.SkillGapAnalyzerService()._analyze_one(conn, "Backend", ["SKILL19"])
        self.assertEqual(result.cv_coverage_score, 0.0)
        self.assertEqual(result.present_skills, ["SKILL19"])
        self.assertEqual(len(result.all_market_skills), 15)
        self.assertEqual(len(result.missing_skills), 10)

    def test_skill_gate_results_and_failure(self):
        gate = self.a.gap.SkillGapAnalyzerService()
        self.assertFalse(gate.has_it_skills([]))
        self.assertFalse(gate.has_it_skills([" "]))
        for rows, expected in [([(1,)], True), ([(0,)], False), ([], False)]:
            with self.subTest(rows=rows):
                conn = self.connection([(["matched"], rows)])
                with patch.object(self.a.trino.dbapi, "connect", return_value=conn):
                    self.assertIs(gate.has_it_skills(["Python"]), expected)
        with patch.object(self.a.trino.dbapi, "connect", side_effect=TimeoutError("fixture timeout")):
            self.assertIsNone(gate.has_it_skills(["Python"]))


if __name__ == "__main__":
    logging.basicConfig(level=logging.CRITICAL)
    unittest.main()
