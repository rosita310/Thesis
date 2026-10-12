"""
Tests for dblp_extract.py. No database, no dump, no pyodbc.

    python test_dblp_extract.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import dblp_extract as dblp

_failures = 0


def check(label, condition):
    global _failures
    if not condition:
        _failures += 1
    print(f"  [{'PASS' if condition else 'FAIL'}] {label}")


def test_ntriples():
    """Lines copied out of the real dump."""
    print("\n-- n-triples parsing --")
    line = ('<https://dblp.org/rec/journals/dke/KumarP23> '
            '<https://dblp.org/rdf/schema#publishedInJournalVolume> "144" .\n')
    check("iri subject, literal object",
          dblp.parse_triple(line) == ("https://dblp.org/rec/journals/dke/KumarP23",
                                      dblp.P_VOLUME, "144"))

    line = ('<https://dblp.org/rec/journals/tcs/WangFW10> '
            '<https://dblp.org/rdf/schema#publishedInJournalVolumeIssue> "40-42" .\n')
    check("a combined issue survives verbatim", dblp.parse_triple(line)[2] == "40-42")

    line = ('<https://dblp.org/rec/journals/dke/KumarP23> '
            '<https://dblp.org/rdf/schema#yearOfPublication> '
            '"2023"^^<http://www.w3.org/2001/XMLSchema#gYear> .\n')
    check("a typed literal drops its ^^type", dblp.parse_triple(line)[2] == "2023")

    line = ('_:Sig_63b7_1 <https://dblp.org/rdf/schema#signatureCreator> '
            '<https://dblp.org/pid/339/2577> .\n')
    check("blank-node subject and iri object",
          dblp.parse_triple(line) == ("_:Sig_63b7_1", dblp.P_SIG_CREATOR,
                                      "https://dblp.org/pid/339/2577"))

    line = ('<https://dblp.org/rec/journals/x/Y1> '
            '<https://dblp.org/rdf/schema#title> "A \\"quoted\\" title." .\n')
    check("escaped quotes inside a literal",
          dblp.parse_triple(line)[2] == 'A "quoted" title.')

    check("a malformed line returns None", dblp.parse_triple("garbage\n") is None)

    line = ('_:Sig_1 <https://dblp.org/rdf/schema#signatureDblpName> '
            r'"Z. Meral \u00D6zsoyoglu" .' + "\n")
    check(r"a \uXXXX escape becomes the real character",
          dblp.parse_triple(line)[2] == "Z. Meral \u00d6zsoyoglu")
    check(r"\UXXXXXXXX is decoded too",
          dblp.nt_unescape(r"x\U0001F600y") == "x\U0001f600y")
    check(r"an escaped backslash is not read as the start of \u",
          dblp.nt_unescape(r"C:\\u0041") == r"C:\u0041")
    check("a literal with no backslash is returned untouched",
          dblp.nt_unescape("plain name") == "plain name")
    check("the simple escapes are decoded too",
          dblp.nt_unescape(r"a\tb\nc\\d") == "a\tb\nc\\d")


def test_predicate_and_key():
    print("\n-- predicate and journal key --")
    check("predicate extracted from an iri-subject line",
          dblp.predicate_of('<https://dblp.org/rec/journals/x/Y1> '
                            f'<{dblp.P_VOLUME}> "1" .') == dblp.P_VOLUME)
    check("predicate extracted from a blank-node-subject line",
          dblp.predicate_of(f'_:Sig_1 <{dblp.P_SIG_NAME}> "A B" .') == dblp.P_SIG_NAME)
    check("publishedInJournal is distinguished from its Volume sibling",
          dblp.predicate_of(f'<s> <{dblp.P_JOURNAL}> "t" .') == dblp.P_JOURNAL
          and dblp.predicate_of(f'<s> <{dblp.P_VOLUME}> "1" .') != dblp.P_JOURNAL)
    check("a line with no predicate iri returns None",
          dblp.predicate_of("garbage") is None)
    check("the needle does not match the Volume/Issue siblings either",
          dblp.NEEDLE_JOURNAL not in
          f'<s> <{dblp.P_VOLUME}> "1" .' + f'<s> <{dblp.P_ISSUE}> "1" .')

    check("the journal key comes from the record uri",
          dblp.journal_key_of("https://dblp.org/rec/journals/tods/Abc23") == "tods")
    check("a non-journal record has no journal key",
          dblp.journal_key_of("https://dblp.org/rec/conf/icmcs/LeeSK07") is None)


def test_iso4_matching():
    print("\n-- iso 4 truncation matching --")
    check("'Trans.' matches 'Transactions'",
          dblp.tokens_compatible(
              dblp.title_tokens("ACM Transactions on Database Systems"),
              dblp.title_tokens("ACM Trans. Database Syst.")))
    check("leading 'ACM' is optional on either side",
          dblp.tokens_compatible(
              dblp.title_tokens("Journal of Data and Information Quality"),
              dblp.title_tokens("ACM J. Data Inf. Qual.")))
    check("trailing 'ACM' is significant and kept",
          dblp.tokens_compatible(dblp.title_tokens("Journal of the ACM"),
                                 dblp.title_tokens("J. ACM")))
    check("different journals do not match",
          not dblp.tokens_compatible(
              dblp.title_tokens("ACM Transactions on Graphics"),
              dblp.title_tokens("ACM Trans. Database Syst.")))
    check("a token-count mismatch never matches",
          not dblp.tokens_compatible(
              dblp.title_tokens("ACM Transactions on Computing Education"),
              dblp.title_tokens("ACM Trans. Comput.")))

    inventory = [
        {"journal_key": "tomccap",
         "journal_title": "ACM Trans. Multim. Comput. Commun. Appl.", "n_articles": "10"},
        {"journal_key": "tomm",
         "journal_title": "ACM Trans. Multim. Comput. Commun. Appl.", "n_articles": "20"},
        {"journal_key": "tods", "journal_title": "ACM Trans. Database Syst.",
         "n_articles": "5"},
    ]
    tomm = "ACM Transactions on Multimedia Computing, Communications, and Applications"
    proposed = dblp.propose_mapping({tomm: "acm"}, inventory)
    check("a renamed journal is flagged `multiple`, both keys kept",
          proposed[0]["confidence"] == "multiple"
          and proposed[0]["dblp_key"] == "tomccap|tomm"
          and proposed[0]["n_articles"] == 30 and proposed[0]["publisher"] == "acm")
    check("an unmatched journal is flagged `none`",
          dblp.propose_mapping({"Totally Made Up Journal": "ieee"},
                               inventory)[0]["confidence"] == "none")
    kept = dblp.propose_mapping({tomm: "acm"}, inventory, {tomm: ["tomm"]})[0]
    check("a verified row is kept as it is",
          kept["confidence"] == "verified" and kept["dblp_key"] == "tomm"
          and kept["n_articles"] == 20)

    matcher = dblp.TitleMatcher(["ACM Transactions on Database Systems"])
    check("the matcher is idempotent and matches",
          matcher.match("ACM Trans. Database Syst.")
          == matcher.match("ACM Trans. Database Syst.")
          == ["ACM Transactions on Database Systems"])
    check("the matcher rejects an unrelated title",
          matcher.match("J. Graph Theory") == [])
    twins = dblp.TitleMatcher(["ACM Transactions on Computing", "Transactions on Computers"])
    check("an ambiguous DBLP title lists every compatible name, in the given order",
          twins.match("ACM Trans. Comput.") == ["ACM Transactions on Computing",
                                                "Transactions on Computers"])
    check("a blank title has no tokens and matches nothing, not everything",
          dblp.title_tokens(None) == () and dblp.title_tokens("") == ()
          and not dblp.tokens_compatible((), ())
          and twins.match("") == [])


def test_candidate_tiers():
    print("\n-- candidate tiers --")
    acm = {"publisher": "acm"}
    check("a plain issue collapses to one candidate",
          dblp.candidate_tiers({**acm, "volume_label": "10", "volume_num": 10,
                                "issue_label": "3", "issue_first_num": 3})
          == [(1, "volume", "10", "3")])
    check("a supplement offers the label first, then the plain numbers",
          dblp.candidate_tiers({**acm, "volume_label": "7S", "volume_num": 7,
                                "issue_label": "1s", "issue_first_num": 1})
          == [(1, "volume", "7S", "1s"), (2, "volume", "7", "1s"),
              (3, "volume", "7S", "1"), (4, "volume", "7", "1")])
    check("a combined issue keeps its label, then falls back to the first number",
          dblp.candidate_tiers({**acm, "volume_label": "37", "volume_num": 37,
                                "issue_label": "1-4", "issue_first_num": 1})
          == [(1, "volume", "37", "1-4"), (3, "volume", "37", "1")])
    check("a missing issue number yields no candidate",
          dblp.candidate_tiers({**acm, "volume_label": "9", "volume_num": 9,
                                "issue_label": None, "issue_first_num": None}) == [])
    check("an IEEE issue is identified by year and issue, label first",
          dblp.candidate_tiers({"publisher": "ieee", "year": 2020,
                                "issue_label": "2_Part_1", "issue_num": 2})
          == [(1, "year", "2020", "2_Part_1"), (2, "year", "2020", "2")])


def test_issue_join():
    print("\n-- the issue join --")
    arts = [{"rec_key": "r1", "journal_key": "tods", "volume": "49", "issue": "2",
             "pagination": "23:1-23:15", "year": "2024", "journal": "J",
             "journal_title": "t", "bibtex_type": "#Article", "title": "x"}]
    index = dblp.index_dblp_issues(arts, {"J": ["tods"]})
    acm = {"publisher": "acm"}
    check("an exact label match is tier 1",
          dblp.match_issue({**acm, "volume_label": "49", "volume_num": 49,
                            "issue_label": "2", "issue_first_num": 2},
                           ["tods"], index)[0] == 1)
    check("a supplement label falls through to the plain volume",
          dblp.match_issue({**acm, "volume_label": "49S", "volume_num": 49,
                            "issue_label": "2", "issue_first_num": 2},
                           ["tods"], index)[0] == 2)
    check("an unknown issue does not match",
          dblp.match_issue({**acm, "volume_label": "49", "volume_num": 49,
                            "issue_label": "9", "issue_first_num": 9},
                           ["tods"], index) is None)
    check("an article without an issue is not indexed",
          dblp.index_dblp_issues([{**arts[0], "issue": ""}], {"J": ["tods"]}) == {})
    check("an IEEE issue is found on year and issue number",
          dblp.match_issue({"publisher": "ieee", "year": 2024, "issue_label": "2_Part_1",
                            "issue_num": 2}, ["tods"], index)[-1] == arts)

    renamed = arts + [{**arts[0], "rec_key": "r2", "journal_key": "tomm", "volume": "49S"}]
    index = dblp.index_dblp_issues(renamed, {"J": ["tods", "tomm"]})
    hit = dblp.match_issue({**acm, "volume_label": "49S", "volume_num": 49,
                            "issue_label": "2", "issue_first_num": 2},
                           ["tods", "tomm"], index)
    check("with two keys, a literal (tier 1) match under the second key beats a "
          "fallback under the first", hit[:2] == (1, "tomm"))


def test_odbc_booleans():
    print("\n-- odbc booleans --")
    check("the driver's '0'/'1' boolean strings are coerced, not taken as truthy",
          dblp.as_bool("1") is True and dblp.as_bool("0") is False
          and dblp.as_bool(True) is True and dblp.as_bool(None) is False
          and dblp.as_bool("t") is True and dblp.as_bool("f") is False)


def test_mapping_file():
    print("\n-- the verified mapping file --")
    f = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8",
                                    newline="")
    f.write("journal,publisher,dblp_key,confidence\nJ One,acm,tods,high\n"
            "J Two,ieee,tomccap|tomm,multiple\nJ None,ieee,,none\n")
    f.close()
    mapping = dblp.load_mapping(Path(f.name))
    check("a mapping row with keys is loaded, a `|`-joined one as several keys",
          mapping == {"J One": ["tods"], "J Two": ["tomccap", "tomm"]})
    check("a journal without a key is left out, so nothing can be `inside` it",
          "J None" not in mapping)
    check("every journal's publisher is read, with or without a key",
          dblp.load_publishers(Path(f.name))
          == {"J One": "acm", "J Two": "ieee", "J None": "ieee"})

    def stops(text):
        tmp = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False,
                                          encoding="utf-8", newline="")
        tmp.write(text)
        tmp.close()
        try:
            dblp.load_mapping(Path(tmp.name))
            return False
        except SystemExit:
            return True

    check("a mapping with no usable key stops the run",
          stops("journal,publisher,dblp_key\nJ,acm,\n"))
    check("a mapping without a publisher column stops the run",
          stops("acm_journal,dblp_key\nJ,tods\n"))
    old = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8",
                                      newline="")
    old.write("acm_journal,dblp_key\nJ,tods|tomm\n")
    old.close()
    check("an old mapping still supplies its verified keys to discover",
          dblp.verified_mapping(Path(old.name)) == {"J": ["tods", "tomm"]})


def main():
    test_ntriples()
    test_predicate_and_key()
    test_iso4_matching()
    test_candidate_tiers()
    test_issue_join()
    test_odbc_booleans()
    test_mapping_file()
    print("\nRESULT:", "ALL PASS" if _failures == 0 else f"{_failures} FAILURE(S)")
    raise SystemExit(1 if _failures else 0)


if __name__ == "__main__":
    main()
