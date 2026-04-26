from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from bdsc_cli.cli import main
from bdsc_cli.core import (
    build_fts_query,
    build_trigram_query,
    build_index,
    detect_query_kind,
    iter_export_rows,
    iter_report_rows,
    list_terms,
    get_status,
    get_stock,
    get_stock_by_rrid,
    lookup_query,
    QueryCriterion,
    search_component,
    search_fbid,
    search_gene,
    search_local,
    search_property,
    search_relationship,
)


BLOOMINGTON = """\
"Stk #","Genotype","Ch # all","A.K.A","Date added","Donor info","Stock comments"
77118,"w[*]; P{10XUAS-Chronos-mVenus}attP2","2","","1/10/2018","Donor: Janelia","optogenetic actuator"
77119,"w[*]; P{10XUAS-CsChrimson}attP2","2","","1/11/2018","Donor: Janelia","red-shifted actuator"
605642,"w[1118]; TI{lexA::p65}Or56a[KO-lexA]","2","","4/23/2026","Donor: BDSC","olfactory LexA line"
605643,"w[1118]; P{GMR13A11-GAL4}Or67d","2","","4/24/2026","Donor: BDSC","olfactory GAL4 line"
"""

COMPONENTS = """\
"Stk #","Genotype","component_symbol","fbid","mapstatement","comment1","comment2","comment3"
77118,"w[*]; P{10XUAS-Chronos-mVenus}attP2","P{10XUAS-Chronos-mVenus}attP2","FBti0195688","","Chronos construct","",""
77119,"w[*]; P{10XUAS-CsChrimson}attP2","P{10XUAS-CsChrimson}attP2","FBti0195689","","CsChrimson construct","",""
605642,"w[1118]; TI{lexA::p65}Or56a[KO-lexA]","TI{lexA::p65}Or56a[KO-lexA]","FBti605642","","Or56a LexA knock-in","",""
605643,"w[1118]; P{GMR13A11-GAL4}Or67d","P{GMR13A11-GAL4}Or67d","FBti605643","","Or67d GAL4 line","",""
"""

STOCKGENES = """\
"stknum","genotype","component_symbol","gene_symbol","fbgn","bdsc_symbol_id","bdsc_gene_id"
77118,"w[*]; P{10XUAS-Chronos-mVenus}attP2","P{10XUAS-Chronos-mVenus}attP2","Chronos","FBgn0000001",1,10
77119,"w[*]; P{10XUAS-CsChrimson}attP2","P{10XUAS-CsChrimson}attP2","CsChrimson","FBgn0000002",2,20
605642,"w[1118]; TI{lexA::p65}Or56a[KO-lexA]","TI{lexA::p65}Or56a[KO-lexA]","Or56a","FBgn0000003",3,30
605643,"w[1118]; P{GMR13A11-GAL4}Or67d","P{GMR13A11-GAL4}Or67d","Or67d","FBgn0000004",4,40
"""

COMPGENES = """\
"bdsc_symbol_id","bdsc_gene_id","compgeneprop_id","prop_syn"
1,10,100,"coding"
2,20,100,"coding"
3,30,100,"coding"
4,40,100,"coding"
"""

COMPPROPS = """\
"bdsc_symbol_id","property_id","property_descrip","prop_syn"
1,200,"optogenetic","opto"
2,200,"optogenetic","opto"
3,300,"LexA driver","lexA"
3,301,"olfactory receptor","olfactory"
4,400,"GAL4 driver","gal4"
4,401,"olfactory receptor","olfactory"
"""


class CoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temp_dir.name)
        raw_dir = self.state_dir / "raw"
        raw_dir.mkdir(parents=True)
        (raw_dir / "bloomington.csv").write_text(BLOOMINGTON, encoding="utf-8")
        (raw_dir / "stockcomps_map_comments.csv").write_text(COMPONENTS, encoding="utf-8")
        (raw_dir / "stockgenes.csv").write_text(STOCKGENES, encoding="utf-8")
        (raw_dir / "stockgenes_compgenes.csv").write_text(COMPGENES, encoding="utf-8")
        (raw_dir / "stockgenes_compprops.csv").write_text(COMPPROPS, encoding="utf-8")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_build_fts_query_tokenizes_text(self) -> None:
        self.assertEqual(build_fts_query("10XUAS-Chronos"), "10xuas* chronos*")

    def test_build_trigram_query_quotes_ngrams(self) -> None:
        self.assertEqual(build_trigram_query("Chronis"), '"chr" OR "hro" OR "ron" OR "oni" OR "nis"')

    def test_build_index_and_search(self) -> None:
        counts = build_index(self.state_dir)
        self.assertEqual(counts["stocks"], 4)
        results = search_local(self.state_dir, "Chronos")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["stknum"], 77118)
        fuzzy_typo = search_local(self.state_dir, "Chronis")
        self.assertEqual(fuzzy_typo[0]["stknum"], 77118)
        fuzzy_spacing = search_local(self.state_dir, "Or56a Lexa")
        self.assertEqual(fuzzy_spacing[0]["stknum"], 605642)

    def test_get_stock_details(self) -> None:
        build_index(self.state_dir)
        stock = get_stock(self.state_dir, 77118)
        assert stock is not None
        self.assertEqual(stock["rrid"], "RRID:BDSC_77118")
        self.assertEqual(stock["genes"][0]["gene_symbol"], "Chronos")

    def test_gene_search_and_status(self) -> None:
        build_index(self.state_dir)
        results = search_gene(self.state_dir, "Chronos")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["fbgn"], "FBgn0000001")
        fuzzy_results = search_gene(self.state_dir, "Chronis")
        self.assertEqual(fuzzy_results[0]["gene_symbol"], "Chronos")
        status = get_status(self.state_dir)
        self.assertTrue(status["db_exists"])
        self.assertEqual(status["index"]["counts"]["stocks"], 4)

    def test_component_fbid_and_rrid_queries(self) -> None:
        build_index(self.state_dir)
        component_results = search_component(self.state_dir, "P{10XUAS-Chronos")
        self.assertEqual(len(component_results), 1)
        self.assertEqual(component_results[0]["fbid"], "FBti0195688")
        self.assertEqual(component_results[0]["property_syns"], "opto")
        self.assertEqual(component_results[0]["gene_relationships"], "coding")
        fuzzy_component_results = search_component(self.state_dir, "Or56a Lexa")
        self.assertEqual(fuzzy_component_results[0]["stknum"], 605642)
        fbid_results = search_fbid(self.state_dir, "FBti0195688")
        self.assertEqual(len(fbid_results), 1)
        self.assertEqual(fbid_results[0]["stknum"], 77118)
        fuzzy_fbid_results = search_fbid(self.state_dir, "60564")
        self.assertEqual(fuzzy_fbid_results[0]["fbid"], "FBti605642")
        stock = get_stock_by_rrid(self.state_dir, "RRID:BDSC_77118")
        assert stock is not None
        self.assertEqual(stock["stknum"], 77118)
        self.assertEqual(stock["components"][0]["property_syns"], "opto")

    def test_detect_query_kind(self) -> None:
        self.assertEqual(detect_query_kind("77118"), "stock")
        self.assertEqual(detect_query_kind("RRID:BDSC_77118"), "rrid")
        self.assertEqual(detect_query_kind("FBgn0000001"), "gene")
        self.assertEqual(detect_query_kind("FBti0195688"), "fbid")
        self.assertEqual(detect_query_kind("P{10XUAS-Chronos"), "component")
        self.assertEqual(detect_query_kind("Chronos"), "gene")

    def test_lookup_query_auto_and_fallback(self) -> None:
        build_index(self.state_dir)
        result = lookup_query(self.state_dir, "RRID:BDSC_77118")
        self.assertEqual(result["kind"], "rrid")
        self.assertEqual(result["results"][0]["stknum"], 77118)
        prop = lookup_query(self.state_dir, "opto", kind="property")
        self.assertEqual(prop["kind"], "property")
        self.assertEqual(prop["results"][0]["stknum"], 77118)
        fallback = lookup_query(self.state_dir, "optogenetic")
        self.assertEqual(fallback["kind"], "search")
        self.assertEqual(fallback["results"][0]["stknum"], 77118)
        typo_fallback = lookup_query(self.state_dir, "Chronis")
        self.assertEqual(typo_fallback["kind"], "gene")
        self.assertEqual(typo_fallback["results"][0]["gene_symbol"], "Chronos")

    def test_property_search(self) -> None:
        build_index(self.state_dir)
        results = search_property(self.state_dir, "opto")
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["property_syns"], "opto")
        fuzzy_results = search_property(self.state_dir, "optogen")
        self.assertEqual(fuzzy_results[0]["property_descriptions"], "optogenetic")

    def test_relationship_search(self) -> None:
        build_index(self.state_dir)
        results = search_relationship(self.state_dir, "coding")
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0]["gene_relationships"], "coding")
        fuzzy_results = search_relationship(self.state_dir, "codng")
        self.assertEqual(fuzzy_results[0]["gene_relationships"], "coding")

    def test_export_rows(self) -> None:
        build_index(self.state_dir)
        stocks = list(iter_export_rows(self.state_dir, "stocks", limit=1))
        self.assertEqual(stocks[0]["rrid"], "RRID:BDSC_77118")
        components = list(iter_export_rows(self.state_dir, "components", limit=1))
        self.assertEqual(components[0]["property_syns"], "opto")
        genes = list(iter_export_rows(self.state_dir, "genes", limit=1))
        self.assertEqual(genes[0]["gene_relationships"], "coding")
        properties = list(iter_export_rows(self.state_dir, "properties", limit=2))
        self.assertEqual(properties[0]["prop_syn"], "opto")

    def test_filtered_export_rows(self) -> None:
        build_index(self.state_dir)
        genes = list(iter_export_rows(self.state_dir, "genes", query="Chronos", kind="gene"))
        self.assertEqual(len(genes), 1)
        self.assertEqual(genes[0]["stknum"], 77118)
        components = list(
            iter_export_rows(self.state_dir, "components", query="FBti0195688", kind="fbid")
        )
        self.assertEqual(len(components), 1)
        self.assertEqual(components[0]["component_symbol"], "P{10XUAS-Chronos-mVenus}attP2")
        properties = list(iter_export_rows(self.state_dir, "properties", query="opto", kind="property"))
        self.assertEqual(len(properties), 2)
        relationships = list(
            iter_export_rows(self.state_dir, "genes", query="coding", kind="relationship")
        )
        self.assertEqual(len(relationships), 4)

    def test_compound_filter_rows(self) -> None:
        build_index(self.state_dir)
        lexa_or56a = list(
            iter_export_rows(
                self.state_dir,
                "components",
                criteria=[
                    QueryCriterion(kind="gene", query="Or56a"),
                    QueryCriterion(kind="property", query="lexA"),
                ],
            )
        )
        self.assertEqual(len(lexa_or56a), 1)
        self.assertEqual(lexa_or56a[0]["stknum"], 605642)

        olfactory_genes = list(
            iter_export_rows(
                self.state_dir,
                "genes",
                criteria=[
                    QueryCriterion(kind="property", query="olfactory"),
                    QueryCriterion(kind="relationship", query="coding"),
                ],
            )
        )
        self.assertEqual({row["gene_symbol"] for row in olfactory_genes}, {"Or56a", "Or67d"})

        no_qf_match = list(
            iter_export_rows(
                self.state_dir,
                "components",
                criteria=[
                    QueryCriterion(kind="gene", query="Or67d"),
                    QueryCriterion(kind="property", query="qf"),
                ],
            )
        )
        self.assertEqual(no_qf_match, [])

    def test_list_terms(self) -> None:
        build_index(self.state_dir)
        props = list_terms(self.state_dir, "properties", limit=5)
        self.assertIn({"term": "opto", "description": "optogenetic", "count": 2}, props)
        relationships = list_terms(self.state_dir, "relationships", limit=5)
        self.assertEqual(relationships[0]["term"], "coding")
        descriptions = list_terms(self.state_dir, "property-descriptions", query="optogenetic", limit=5)
        self.assertEqual(descriptions[0]["synonym"], "opto")

    def test_lookup_relationship_kind(self) -> None:
        build_index(self.state_dir)
        result = lookup_query(self.state_dir, "coding", kind="relationship")
        self.assertEqual(result["kind"], "relationship")
        self.assertEqual(result["result_count"], 4)

    def test_filter_command_json(self) -> None:
        build_index(self.state_dir)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "filter",
                    "--state-dir",
                    str(self.state_dir),
                    "--gene",
                    "Or56a",
                    "--property",
                    "lexA",
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        self.assertIn("605642", stdout.getvalue())

    def test_canned_reports(self) -> None:
        build_index(self.state_dir)
        olfactory = list(iter_report_rows(self.state_dir, "olfactory", dataset="components"))
        self.assertEqual({row["stknum"] for row in olfactory}, {605642, 605643})

        drivers = list(iter_report_rows(self.state_dir, "drivers", dataset="components"))
        self.assertEqual({row["stknum"] for row in drivers}, {605642, 605643})

        optogenetics = list(iter_report_rows(self.state_dir, "optogenetics", dataset="components"))
        self.assertEqual({row["stknum"] for row in optogenetics}, {77118, 77119})

    def test_report_command_json(self) -> None:
        build_index(self.state_dir)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "report",
                    "olfactory",
                    "--state-dir",
                    str(self.state_dir),
                    "--dataset",
                    "genes",
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        self.assertIn("Or56a", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
