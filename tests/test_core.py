from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from bdsc_cli.core import (
    build_fts_query,
    build_index,
    detect_query_kind,
    iter_export_rows,
    list_terms,
    get_status,
    get_stock,
    get_stock_by_rrid,
    lookup_query,
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
"""

COMPONENTS = """\
"Stk #","Genotype","component_symbol","fbid","mapstatement","comment1","comment2","comment3"
77118,"w[*]; P{10XUAS-Chronos-mVenus}attP2","P{10XUAS-Chronos-mVenus}attP2","FBti0195688","","Chronos construct","",""
77119,"w[*]; P{10XUAS-CsChrimson}attP2","P{10XUAS-CsChrimson}attP2","FBti0195689","","CsChrimson construct","",""
"""

STOCKGENES = """\
"stknum","genotype","component_symbol","gene_symbol","fbgn","bdsc_symbol_id","bdsc_gene_id"
77118,"w[*]; P{10XUAS-Chronos-mVenus}attP2","P{10XUAS-Chronos-mVenus}attP2","Chronos","FBgn0000001",1,10
77119,"w[*]; P{10XUAS-CsChrimson}attP2","P{10XUAS-CsChrimson}attP2","CsChrimson","FBgn0000002",2,20
"""

COMPGENES = """\
"bdsc_symbol_id","bdsc_gene_id","compgeneprop_id","prop_syn"
1,10,100,"coding"
2,20,100,"coding"
"""

COMPPROPS = """\
"bdsc_symbol_id","property_id","property_descrip","prop_syn"
1,200,"optogenetic","opto"
2,200,"optogenetic","opto"
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

    def test_build_index_and_search(self) -> None:
        counts = build_index(self.state_dir)
        self.assertEqual(counts["stocks"], 2)
        results = search_local(self.state_dir, "Chronos")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["stknum"], 77118)

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
        status = get_status(self.state_dir)
        self.assertTrue(status["db_exists"])
        self.assertEqual(status["index"]["counts"]["stocks"], 2)

    def test_component_fbid_and_rrid_queries(self) -> None:
        build_index(self.state_dir)
        component_results = search_component(self.state_dir, "P{10XUAS-Chronos")
        self.assertEqual(len(component_results), 1)
        self.assertEqual(component_results[0]["fbid"], "FBti0195688")
        self.assertEqual(component_results[0]["property_syns"], "opto")
        self.assertEqual(component_results[0]["gene_relationships"], "coding")
        fbid_results = search_fbid(self.state_dir, "FBti0195688")
        self.assertEqual(len(fbid_results), 1)
        self.assertEqual(fbid_results[0]["stknum"], 77118)
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

    def test_property_search(self) -> None:
        build_index(self.state_dir)
        results = search_property(self.state_dir, "opto")
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["property_syns"], "opto")

    def test_relationship_search(self) -> None:
        build_index(self.state_dir)
        results = search_relationship(self.state_dir, "coding")
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["gene_relationships"], "coding")

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
        self.assertEqual(len(relationships), 2)

    def test_list_terms(self) -> None:
        build_index(self.state_dir)
        props = list_terms(self.state_dir, "properties", limit=5)
        self.assertEqual(props[0]["term"], "opto")
        self.assertEqual(props[0]["count"], 2)
        relationships = list_terms(self.state_dir, "relationships", limit=5)
        self.assertEqual(relationships[0]["term"], "coding")
        descriptions = list_terms(self.state_dir, "property-descriptions", query="optogenetic", limit=5)
        self.assertEqual(descriptions[0]["synonym"], "opto")

    def test_lookup_relationship_kind(self) -> None:
        build_index(self.state_dir)
        result = lookup_query(self.state_dir, "coding", kind="relationship")
        self.assertEqual(result["kind"], "relationship")
        self.assertEqual(result["result_count"], 2)


if __name__ == "__main__":
    unittest.main()
