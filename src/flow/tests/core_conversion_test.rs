use flow::{StreamSqlConverter, DataFusionEvaluator, ScalarExpr};
use flow::tuple::Tuple;
use flow::row::Row;
use datatypes::{Schema, ColumnSchema, ConcreteDatatype, Int64Type, Value};
use parser::parse_sql;

#[test]
fn test_core_conversion_flow() {
    println!("\n=== 核心转换流程测试 ===");
    println!("核心演示：SelectStmt → ScalarExpr → 计算结果");
    println!("测试 SQL: SELECT a+b, 42");
    
    // 1. 创建测试schema
    let schema = Schema::new(vec![
        ColumnSchema::new("a".to_string(), ConcreteDatatype::Int64(Int64Type)),
        ColumnSchema::new("b".to_string(), ConcreteDatatype::Int64(Int64Type)),
    ]);
    
    // 2. 使用parser模块解析SQL，得到SelectStmt
    println!("\n🔍 步骤1: 使用parser模块解析SQL");
    let sql = "SELECT a+b, 42";
    println!("输入SQL: {}", sql);
    
    let select_stmt = parse_sql(sql).expect("StreamDialect解析应该成功");
    println!("✓ 成功得到SelectStmt，包含 {} 个字段", select_stmt.select_fields.len());
    
    // 3. 查看SelectStmt结构（验证输入正确）
    println!("\n🔍 步骤2: SelectStmt结构验证");
    for (i, field) in select_stmt.select_fields.iter().enumerate() {
        println!("  字段 {}: {:?}", i + 1, field.expr);
        println!("         别名: {:?}", field.alias);
    }
    
    // 4. 核心转换：使用StreamSqlConverter将SelectStmt转换为ScalarExpr
    println!("\n🔍 步骤3: 核心转换 - SelectStmt → ScalarExpr");
    let converter = StreamSqlConverter::new();
    let expressions = converter.convert_select_stmt_to_scalar(&select_stmt, &schema)
        .expect("SelectStmt转换应该成功");
    
    // 5. 验证转换结果
    println!("✓ 成功得到 {} 个ScalarExpr", expressions.len());
    assert_eq!(expressions.len(), 2, "应该得到2个表达式");
    
    // 6. 详细验证每个表达式
    println!("\n🔍 表达式详细验证");
    
    // 第一个表达式：a + b
    match &expressions[0] {
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            println!("✓ 第一个表达式是二元操作: {:?}", func);
            assert_eq!(*func, flow::expr::BinaryFunc::Add, "应该是加法操作");
            
            // 验证操作数映射
            match (expr1.as_ref(), expr2.as_ref()) {
                (ScalarExpr::Column(idx1), ScalarExpr::Column(idx2)) => {
                    println!("✓ 操作数正确映射到列 {} + {}", idx1, idx2);
                    assert_eq!(*idx1, 0, "第一个操作数应该是列0 (a)");
                    assert_eq!(*idx2, 1, "第二个操作数应该是列1 (b)");
                }
                _ => panic!("操作数应该是列引用"),
            }
        }
        _ => panic!("第一个表达式应该是二元操作"),
    }
    
    // 第二个表达式：42 (字面量)
    match &expressions[1] {
        ScalarExpr::Literal(val, _) => {
            println!("✓ 第二个表达式是字面量: {:?}", val);
            assert_eq!(*val, Value::Int64(42), "应该是整数42");
        }
        _ => panic!("第二个表达式应该是字面量"),
    }
    
    // 7. 创建测试数据进行计算验证
    println!("\n🔍 步骤4: 计算结果验证");
    let evaluator = DataFusionEvaluator::new();
    let test_data = Row::from(vec![
        Value::Int64(5),  // a = 5
        Value::Int64(3),  // b = 3
    ]);
    let tuple = Tuple::new(schema, test_data);
    
    // 计算第一个表达式：a + b = 5 + 3 = 8
    let result1 = expressions[0].eval(&evaluator, &tuple).expect("计算应该成功");
    println!("✓ 表达式1 (a+b) 计算结果: {:?}", result1);
    assert_eq!(result1, Value::Int64(8), "a+b 应该等于 8");
    
    // 计算第二个表达式：42 (字面量)
    let result2 = expressions[1].eval(&evaluator, &tuple).expect("计算应该成功");
    println!("✓ 表达式2 (42) 计算结果: {:?}", result2);
    assert_eq!(result2, Value::Int64(42), "字面量42 应该等于 42");
    
    println!("\n✅ 核心转换流程测试完成！");
    println!("🎯 验证结果：parser → SelectStmt → StreamSqlConverter → ScalarExpr → 计算结果");
    println!("   整个流程完全正确！");
}