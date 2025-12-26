use datatypes::{
    ColumnSchema, ConcreteDatatype, Int64Type, Schema, StringType, StructField, StructType,
};
use flow::planner::logical::create_logical_plan;
use flow::sql_conversion::{SchemaBinding, SchemaBindingEntry, SourceBindingKind};
use flow::{
    MqttStreamProps, PipelineExplain, PipelineRegistries, StreamDecoderConfig, StreamDefinition,
    StreamProps,
};
use parser::parse_sql_with_registries;
use std::collections::HashMap;
use std::sync::Arc;

fn setup_streams() -> HashMap<String, Arc<StreamDefinition>> {
    let stream_schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        "stream".to_string(),
        "a".to_string(),
        ConcreteDatatype::Int64(Int64Type),
    )]));
    let stream_def = StreamDefinition::new(
        "stream",
        Arc::clone(&stream_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let user_struct = ConcreteDatatype::Struct(StructType::new(Arc::new(vec![
        StructField::new("c".to_string(), ConcreteDatatype::Int64(Int64Type), false),
        StructField::new("d".to_string(), ConcreteDatatype::String(StringType), false),
    ])));
    let stream_2_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "stream_2".to_string(),
            "a".to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new("stream_2".to_string(), "b".to_string(), user_struct),
    ]));
    let stream_2_def = StreamDefinition::new(
        "stream_2",
        Arc::clone(&stream_2_schema),
        StreamProps::Mqtt(MqttStreamProps::default()),
        StreamDecoderConfig::json(),
    );

    let mut stream_defs = HashMap::new();
    stream_defs.insert("stream".to_string(), Arc::new(stream_def));
    stream_defs.insert("stream_2".to_string(), Arc::new(stream_2_def));

    stream_defs
}

fn bindings_for_select(
    select_stmt: &parser::SelectStmt,
    stream_defs: &HashMap<String, Arc<StreamDefinition>>,
) -> SchemaBinding {
    SchemaBinding::new(
        select_stmt
            .source_infos
            .iter()
            .map(|source| {
                let def = stream_defs
                    .get(&source.name)
                    .unwrap_or_else(|| panic!("missing stream definition: {}", source.name));
                SchemaBindingEntry {
                    source_name: source.name.clone(),
                    alias: source.alias.clone(),
                    schema: def.schema(),
                    kind: SourceBindingKind::Regular,
                }
            })
            .collect(),
    )
}

fn explain_json_string(sql: &str) -> String {
    let registries = PipelineRegistries::new_with_builtin();
    let stream_defs = setup_streams();

    let select_stmt = parse_sql_with_registries(
        sql,
        registries.aggregate_registry(),
        registries.stateful_registry(),
    )
    .expect("parse sql");

    let bindings = bindings_for_select(&select_stmt, &stream_defs);
    let logical_plan = create_logical_plan(select_stmt, vec![], &stream_defs).expect("logical");

    let (logical_plan, bindings) = flow::optimize_logical_plan(logical_plan, &bindings);

    let physical_plan =
        flow::create_physical_plan(Arc::clone(&logical_plan), &bindings, &registries)
            .expect("physical");

    let physical_plan = flow::optimize_physical_plan(
        physical_plan,
        registries.encoder_registry().as_ref(),
        registries.aggregate_registry(),
    );
    let explain = PipelineExplain::new(logical_plan, physical_plan);
    println!("{}", sql);
    println!("{}", explain.to_pretty_string());
    explain.to_json().to_string()
}

#[test]
fn plan_explain_table_driven() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        expected: &'static str,
    }

    let cases = vec![
        Case {
            name: "stateful_select_only",
            sql: "SELECT lag(a) FROM stream",
            expected: r##"{"logical":{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Project_2","info":["fields=[col_1]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProject_3","info":["fields=[col_1]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "stateful_where_before_project",
            sql: "SELECT a FROM stream WHERE lag(a) > 0",
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Filter_2","info":["predicate=col_1 > 0"],"operator":"Filter"}],"id":"Project_3","info":["fields=[a]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalFilter_3","info":["predicate=col_1 > 0"],"operator":"PhysicalFilter"}],"id":"PhysicalProject_4","info":["fields=[a]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "stateful_before_window_and_aggregation",
            sql: "SELECT sum(a), lag(a) FROM stream GROUP BY tumblingwindow('ss', 10)",
            expected: r##"{"logical":{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"DataSource_0","info":["source=stream","decoder=json","schema=[a]"],"operator":"DataSource"}],"id":"StatefulFunction_1","info":["calls=[lag(a) -> col_1]"],"operator":"StatefulFunction"}],"id":"Window_2","info":["kind=tumbling","unit=Seconds","length=10"],"operator":"Window"}],"id":"Aggregation_3","info":["aggregates=[sum(a) -> col_2]"],"operator":"Aggregation"}],"id":"Project_4","info":["fields=[col_2; col_1]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream","schema=[a]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a]"],"operator":"PhysicalDecoder"}],"id":"PhysicalStatefulFunction_2","info":["calls=[lag(a) -> col_1]"],"operator":"PhysicalStatefulFunction"}],"id":"PhysicalProcessTimeWatermark_3","info":["window=tumbling","unit=Seconds","length=10","mode=processing_time","interval=10"],"operator":"PhysicalProcessTimeWatermark"}],"id":"PhysicalStreamingAggregation_5","info":["calls=[sum(a) -> col_2]","window=tumbling","unit=Seconds","length=10"],"operator":"PhysicalStreamingAggregation"}],"id":"PhysicalProject_6","info":["fields=[col_2; col_1]"],"operator":"PhysicalProject"}}"##,
        },
        Case {
            name: "physical_explain_reflects_pruned_struct_schema",
            sql: "SELECT stream_2.a, stream_2.b->c FROM stream_2",
            expected: r##"{"logical":{"children":[{"children":[],"id":"DataSource_0","info":["source=stream_2","decoder=json","schema=[a, b{c}]"],"operator":"DataSource"}],"id":"Project_1","info":["fields=[stream_2.a; stream_2.b -> c]"],"operator":"Project"},"options":null,"physical":{"children":[{"children":[{"children":[],"id":"PhysicalDataSource_0","info":["source=stream_2","schema=[a, b{c}]"],"operator":"PhysicalDataSource"}],"id":"PhysicalDecoder_1","info":["decoder=json","schema=[a, b{c}]"],"operator":"PhysicalDecoder"}],"id":"PhysicalProject_2","info":["fields=[stream_2.a; stream_2.b -> c]"],"operator":"PhysicalProject"}}"##,
        },
    ];

    for case in cases {
        let got = explain_json_string(case.sql);
        assert_eq!(got, case.expected, "case={}", case.name);
    }
}
