from __future__ import annotations

import contextlib
import io
import json
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
    search_driver_family,
    search_fbid,
    search_gene,
    search_local,
    search_property,
    search_property_exact,
    search_relationship,
)


BLOOMINGTON = """\
"Stk #","Genotype","Ch # all","A.K.A","Date added","Donor info","Stock comments"
77118,"w[*]; P{10XUAS-Chronos-mVenus}attP2","2","","1/10/2018","Donor: Janelia","optogenetic actuator"
77119,"w[*]; P{10XUAS-CsChrimson}attP2","2","","1/11/2018","Donor: Janelia","red-shifted actuator"
605642,"w[1118]; TI{lexA::p65}Or56a[KO-lexA]","2","","4/23/2026","Donor: BDSC","olfactory LexA line"
605643,"w[1118]; P{GMR13A11-GAL4}Or67d","2","","4/24/2026","Donor: BDSC","olfactory GAL4 line"
605644,"w[1118]; TI{lexA::p65}Or42b[KO-lexA]","2","","4/25/2026","Donor: BDSC","Or42b LexA line"
605645,"w[1118]; P{Or42b-QF2}attP40","2","","4/25/2026","Donor: BDSC","Or42b QF line"
605646,"w[1118]; P{VT012282-GAL4.DBD}attP2","2","","4/25/2026","Donor: BDSC","Or42b split driver"
605647,"w[1118]; P{Fake-GAL4}Foo/CyO","2","","4/25/2026","Donor: BDSC","multi-component GAL4 stock"
605648,"w[1118]; P{UAS-PlexA.W}3","3","","4/25/2026","Donor: BDSC","PlexA control"
82182,"w[1118]; P{y[+t7.7] w[+mC]=20XUAS-CsChrimson.mCherry}su(Hw)attP1","1;3","","5/21/2019","Donor: Janelia",""
82183,"PBac{y[+mDint2] w[+mC]=13XLexAop2-IVS-CsChrimson.tdTomato}VK00005","3","","5/21/2019","Donor: Janelia",""
"""

COMPONENTS = """\
"Stk #","Genotype","component_symbol","fbid","mapstatement","comment1","comment2","comment3"
77118,"w[*]; P{10XUAS-Chronos-mVenus}attP2","P{10XUAS-Chronos-mVenus}attP2","FBti0195688","","Chronos construct","",""
77119,"w[*]; P{10XUAS-CsChrimson}attP2","P{10XUAS-CsChrimson}attP2","FBti0195689","","CsChrimson construct","",""
605642,"w[1118]; TI{lexA::p65}Or56a[KO-lexA]","TI{lexA::p65}Or56a[KO-lexA]","FBti605642","","Or56a LexA knock-in","",""
605643,"w[1118]; P{GMR13A11-GAL4}Or67d","P{GMR13A11-GAL4}Or67d","FBti605643","","Or67d GAL4 line","",""
605644,"w[1118]; TI{lexA::p65}Or42b[KO-lexA]","TI{lexA::p65}Or42b[KO-lexA]","FBti605644","","Or42b LexA knock-in","",""
605645,"w[1118]; P{Or42b-QF2}attP40","P{Or42b-QF2}attP40","FBti605645","","Or42b QF line","",""
605646,"w[1118]; P{VT012282-GAL4.DBD}attP2","P{VT012282-GAL4.DBD}attP2","FBti605646","","Or42b split driver","",""
605647,"w[1118]; P{Fake-GAL4}Foo/CyO","P{Fake-GAL4}Foo","FBti605647","","Foo GAL4 driver","",""
605647,"w[1118]; P{Fake-GAL4}Foo/CyO","CyO","FBab605647","","Balancer sibling","",""
605648,"w[1118]; P{UAS-PlexA.W}3","P{UAS-PlexA.W}3","FBti605648","","PlexA expression construct","",""
82182,"w[1118]; P{y[+t7.7] w[+mC]=20XUAS-CsChrimson.mCherry}su(Hw)attP1","P{20XUAS-CsChrimson.mCherry}su(Hw)attP1","FBti0196629","Chr 3","Expresses an mCherry-tagged red-shifted channelrhodopsin under the control of UAS.","",""
82183,"PBac{y[+mDint2] w[+mC]=13XLexAop2-IVS-CsChrimson.tdTomato}VK00005","PBac{13XLexAop2-IVS-CsChrimson.tdTomato}VK00005","FBti0204689","Chr 3","Expresses a tdTomato-tagged red-shifted channelrhodopsin under the control of the lexA operator.","",""
"""

STOCKGENES = """\
"stknum","genotype","component_symbol","gene_symbol","fbgn","bdsc_symbol_id","bdsc_gene_id"
77118,"w[*]; P{10XUAS-Chronos-mVenus}attP2","P{10XUAS-Chronos-mVenus}attP2","Chronos","FBgn0000001",1,10
77119,"w[*]; P{10XUAS-CsChrimson}attP2","P{10XUAS-CsChrimson}attP2","CsChrimson","FBgn0000002",2,20
605642,"w[1118]; TI{lexA::p65}Or56a[KO-lexA]","TI{lexA::p65}Or56a[KO-lexA]","Or56a","FBgn0000003",3,30
605643,"w[1118]; P{GMR13A11-GAL4}Or67d","P{GMR13A11-GAL4}Or67d","Or67d","FBgn0000004",4,40
605644,"w[1118]; TI{lexA::p65}Or42b[KO-lexA]","TI{lexA::p65}Or42b[KO-lexA]","Or42b","FBgn0000005",5,50
605645,"w[1118]; P{Or42b-QF2}attP40","P{Or42b-QF2}attP40","Or42b","FBgn0000005",6,50
605646,"w[1118]; P{VT012282-GAL4.DBD}attP2","P{VT012282-GAL4.DBD}attP2","Or42b","FBgn0000005",7,50
605647,"w[1118]; P{Fake-GAL4}Foo/CyO","P{Fake-GAL4}Foo","Foo","FBgn0000006",8,60
605648,"w[1118]; P{UAS-PlexA.W}3","P{UAS-PlexA.W}3","PlexA","FBgn0000007",11,70
82182,"w[1118]; P{y[+t7.7] w[+mC]=20XUAS-CsChrimson.mCherry}su(Hw)attP1","P{20XUAS-CsChrimson.mCherry}su(Hw)attP1","CsChrimson","FBto0000558",9,20
82183,"PBac{y[+mDint2] w[+mC]=13XLexAop2-IVS-CsChrimson.tdTomato}VK00005","PBac{13XLexAop2-IVS-CsChrimson.tdTomato}VK00005","CsChrimson","FBto0000558",10,20
"""

COMPGENES = """\
"bdsc_symbol_id","bdsc_gene_id","compgeneprop_id","prop_syn"
1,10,100,"coding"
2,20,100,"coding"
3,30,100,"coding"
4,40,100,"coding"
5,50,100,"coding"
6,50,100,"coding"
7,50,100,"coding"
8,60,100,"coding"
9,20,100,"coding"
10,20,100,"coding"
"""

COMPPROPS = """\
"bdsc_symbol_id","property_id","property_descrip","prop_syn"
1,200,"optogenetic","opto"
2,200,"optogenetic","opto"
3,300,"LexA driver","lexA"
3,301,"olfactory receptor","olfactory"
4,400,"GAL4 driver","gal4"
4,401,"olfactory receptor","olfactory"
5,500,"This component is a lexA-bearing insertion that could potentially report expression of nearby genes.","lexA reporter - putative"
6,600,"This components is a transgene carrying QF","QF"
7,700,"This component expresses leucine zipper-based hemi driver of either the DNA-binding or activation domain of GAL4, QF, p65, lexA etc for use in DBDzip and ADzip combination.","split zip hemi driver"
8,800,"GAL4 driver","GAL4"
9,900,"This component contains mCherry.","mCherry"
10,901,"This component contains Tomato.","Tomato"
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

    def add_many_cschrimson_rows(self, count: int = 60) -> None:
        raw_dir = self.state_dir / "raw"
        bloomington_rows = []
        component_rows = []
        stockgene_rows = []
        for index in range(count):
            stknum = 78000 + index
            component = f"P{{20XUAS-CsChrimson.synthetic{index}}}attP2"
            genotype = f"w[1118]; {component}"
            bloomington_rows.append(
                f'{stknum},"{genotype}","2","","5/21/2019","Donor: Janelia","synthetic CsChrimson row"'
            )
            component_rows.append(
                f'{stknum},"{genotype}","{component}","FBti78{index:04d}","","CsChrimson synthetic construct","",""'
            )
            stockgene_rows.append(
                f'{stknum},"{genotype}","{component}","CsChrimson","FBto0000558",{78000 + index},20'
            )

        for filename, rows in (
            ("bloomington.csv", bloomington_rows),
            ("stockcomps_map_comments.csv", component_rows),
            ("stockgenes.csv", stockgene_rows),
        ):
            path = raw_dir / filename
            path.write_text(path.read_text(encoding="utf-8") + "\n".join(rows) + "\n", encoding="utf-8")

    def assert_cli_error_shows_help(self, argv: list[str], *expected: str) -> str:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as raised:
                main(argv)
        self.assertEqual(raised.exception.code, 2)
        output = stderr.getvalue()
        for text in expected:
            self.assertIn(text, output)
        return output

    def test_build_fts_query_tokenizes_text(self) -> None:
        self.assertEqual(build_fts_query("10XUAS-Chronos"), "10xuas* chronos*")

    def test_build_trigram_query_quotes_ngrams(self) -> None:
        self.assertEqual(build_trigram_query("Chronis"), '"chr" OR "hro" OR "ron" OR "oni" OR "nis"')

    def test_build_index_and_search(self) -> None:
        counts = build_index(self.state_dir)
        self.assertEqual(counts["stocks"], 11)
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
        self.assertEqual(status["index"]["counts"]["stocks"], 11)

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
        self.assertEqual(detect_query_kind("cschrimson.tdtomato"), "search")
        self.assertEqual(detect_query_kind("lexaop cschrimson"), "search")
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
        construct = lookup_query(self.state_dir, "cschrimson.tdtomato")
        self.assertEqual(construct["kind"], "search")
        self.assertEqual(construct["results"][0]["stknum"], 82183)
        lexical_construct = lookup_query(self.state_dir, "lexaop cschrimson")
        self.assertEqual(lexical_construct["kind"], "search")
        self.assertIn(82183, {row["stknum"] for row in lexical_construct["results"]})

    def test_property_search(self) -> None:
        build_index(self.state_dir)
        results = search_property(self.state_dir, "opto")
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["property_syns"], "opto")
        fuzzy_results = search_property(self.state_dir, "optogen")
        self.assertEqual(fuzzy_results[0]["property_descriptions"], "optogenetic")
        noisy_lexa = search_property(self.state_dir, "lexA")
        self.assertEqual({row["stknum"] for row in noisy_lexa}, {605642, 605644, 605646})
        exact_lexa = search_property_exact(self.state_dir, "lexA")
        self.assertEqual({row["stknum"] for row in exact_lexa}, {605642})
        family_lexa = search_driver_family(self.state_dir, "lexA")
        self.assertEqual({row["stknum"] for row in family_lexa}, {605642, 605644})
        self.assertNotIn(605648, {row["stknum"] for row in family_lexa})
        family_qf = search_driver_family(self.state_dir, "QF")
        self.assertEqual({row["stknum"] for row in family_qf}, {605645})
        family_gal4_components = {row["component_symbol"] for row in search_driver_family(self.state_dir, "GAL4")}
        self.assertIn("P{Fake-GAL4}Foo", family_gal4_components)
        self.assertNotIn("CyO", family_gal4_components)

    def test_relationship_search(self) -> None:
        build_index(self.state_dir)
        results = search_relationship(self.state_dir, "coding")
        self.assertEqual(len(results), 10)
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
        self.assertEqual(len(relationships), 10)

        exact_lexa = list(
            iter_export_rows(self.state_dir, "components", query="lexA", kind="property-exact")
        )
        self.assertEqual({row["stknum"] for row in exact_lexa}, {605642})
        family_lexa = list(
            iter_export_rows(self.state_dir, "components", query="lexA", kind="driver-family")
        )
        self.assertEqual({row["stknum"] for row in family_lexa}, {605642, 605644})

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
        self.assertEqual(result["result_count"], 10)
        exact_property = lookup_query(self.state_dir, "lexA", kind="property-exact")
        self.assertEqual({row["stknum"] for row in exact_property["results"]}, {605642})
        family = lookup_query(self.state_dir, "qf", kind="driver-family")
        self.assertEqual({row["stknum"] for row in family["results"]}, {605645})

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

    def test_find_command_lookup_mode(self) -> None:
        build_index(self.state_dir)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "find",
                    "Chronis",
                    "--state-dir",
                    str(self.state_dir),
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = stdout.getvalue()
        self.assertIn('"kind": "gene"', payload)
        self.assertIn('"Chronos"', payload)

    def test_find_and_search_have_no_default_limit(self) -> None:
        self.add_many_cschrimson_rows()
        build_index(self.state_dir)

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "find",
                    "CsChrimson",
                    "--state-dir",
                    str(self.state_dir),
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        find_payload = json.loads(stdout.getvalue())
        self.assertEqual(find_payload["result_count"], 63)
        self.assertIn(82182, {row["stknum"] for row in find_payload["results"]})

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "search",
                    "CsChrimson",
                    "--state-dir",
                    str(self.state_dir),
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        search_payload = json.loads(stdout.getvalue())
        self.assertEqual(len(search_payload), 63)
        self.assertIn(82182, {row["stknum"] for row in search_payload})

    def test_missing_arguments_show_relevant_help(self) -> None:
        self.assert_cli_error_shows_help([], "usage: bdsc", "find", "stock")
        self.assert_cli_error_shows_help(
            ["export"],
            "usage: bdsc export",
            "stocks",
            "error: the following arguments are required: dataset",
        )
        self.assert_cli_error_shows_help(
            ["stock"],
            "usage: bdsc stock",
            "stknum",
            "error: the following arguments are required: stknum",
        )
        self.assert_cli_error_shows_help(
            ["find"],
            "usage: bdsc find",
            "--gene",
            "error: find requires a query or at least one filter flag",
        )
        self.assert_cli_error_shows_help(
            ["filter"],
            "usage: bdsc filter",
            "--gene",
            "error: filter requires at least one filter flag",
        )
        self.assert_cli_error_shows_help(
            ["lookup"],
            "usage: bdsc lookup",
            "--input",
            "error: lookup requires at least one query or --input",
        )

    def test_find_command_filter_mode(self) -> None:
        build_index(self.state_dir)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "find",
                    "--state-dir",
                    str(self.state_dir),
                    "--gene",
                    "Or56a",
                    "--property-exact",
                    "lexA",
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = stdout.getvalue()
        self.assertIn("605642", payload)
        self.assertNotIn("605644", payload)

    def test_find_command_dataset_override(self) -> None:
        build_index(self.state_dir)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "find",
                    "Or42b",
                    "--state-dir",
                    str(self.state_dir),
                    "--dataset",
                    "genes",
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        payload = stdout.getvalue()
        self.assertIn('"gene_symbol": "Or42b"', payload)
        self.assertNotIn('"kind"', payload)

    def test_exact_property_and_driver_family_commands(self) -> None:
        build_index(self.state_dir)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "property-exact",
                    "lexA",
                    "--state-dir",
                    str(self.state_dir),
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        self.assertIn("605642", stdout.getvalue())
        self.assertNotIn("605644", stdout.getvalue())

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "driver-family",
                    "qf",
                    "--state-dir",
                    str(self.state_dir),
                    "--json",
                ]
            )
        self.assertEqual(exit_code, 0)
        self.assertIn("605645", stdout.getvalue())
        self.assertNotIn("605646", stdout.getvalue())

    def test_status_command_json_flag(self) -> None:
        build_index(self.state_dir)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["status", "--state-dir", str(self.state_dir), "--json"])
        self.assertEqual(exit_code, 0)
        self.assertIn('"db_exists": true', stdout.getvalue())

    def test_top_level_help_hides_legacy_commands(self) -> None:
        stdout = io.StringIO()
        with self.assertRaises(SystemExit) as exc:
            with contextlib.redirect_stdout(stdout):
                main(["--help"])
        self.assertEqual(exc.exception.code, 0)
        help_text = stdout.getvalue()
        self.assertIn("find", help_text)
        self.assertNotIn("==SUPPRESS==", help_text)
        self.assertNotIn("live-search", help_text)

    def test_canned_reports(self) -> None:
        build_index(self.state_dir)
        olfactory = list(iter_report_rows(self.state_dir, "olfactory", dataset="components"))
        self.assertEqual({row["stknum"] for row in olfactory}, {605642, 605643, 605644, 605645})

        drivers = list(iter_report_rows(self.state_dir, "drivers", dataset="components"))
        self.assertEqual({row["stknum"] for row in drivers}, {605642, 605643, 605644, 605645, 605646, 605647})
        self.assertNotIn("CyO", {row["component_symbol"] for row in drivers})

        optogenetics = list(iter_report_rows(self.state_dir, "optogenetics", dataset="components"))
        self.assertEqual({row["stknum"] for row in optogenetics}, {77118, 77119, 82182, 82183})

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
