import importlib.util
import pathlib
import sys
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "check_sql_guardrails.py"
SPEC = importlib.util.spec_from_file_location("check_sql_guardrails", SCRIPT)
CHECKER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CHECKER
assert SPEC.loader is not None
SPEC.loader.exec_module(CHECKER)


class SqlLiteralDetectionTests(unittest.TestCase):
    def test_plain_language_evaluator_rubric_is_not_sql(self):
        self.assertFalse(
            CHECKER.is_sql_like(
                "Before issuing a refund, verify the ticket is eligible and explain the result."
            )
        )

    def test_structured_session_status_output_is_not_sql(self):
        self.assertFalse(
            CHECKER.is_sql_like(
                "agent={agent_name} session_id={session_id} trace_id={trace_id} tool={tool}"
            )
        )

    def test_explain_query_is_sql(self):
        query = "EXPLAIN" + " ANALYZE" + " SELECT * " + "FROM " + "traces"
        self.assertTrue(CHECKER.is_sql_like(query))

    def test_bare_pragma_command_is_sql(self):
        self.assertTrue(CHECKER.is_sql_like("PRAGMA" + " enable_profiling"))

    def test_prose_mentioning_pragma_is_not_sql(self):
        self.assertFalse(CHECKER.is_sql_like("The pragma setting controls profiling."))

    def test_session_id_filter_is_sql(self):
        query = "SELECT * " + "FROM sessions WHERE " + "session_id " + "= ?"
        self.assertTrue(CHECKER.is_sql_like(query))

    def test_sql_literal_extractor_keeps_inline_queries_visible(self):
        query = "SELECT * " + "FROM sessions WHERE " + "session_id " + "= ?"
        source = 'query = "' + query + '"'
        self.assertEqual(CHECKER.sql_literals(source, ".py"), [(query, 1)])


if __name__ == "__main__":
    unittest.main()
