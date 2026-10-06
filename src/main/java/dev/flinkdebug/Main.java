package dev.flinkdebug;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.flinkdebug.core.*;
import dev.flinkdebug.ui.StateLensFrame;
import org.apache.flink.runtime.util.EnvironmentInformation;

import javax.swing.*;
import java.awt.GraphicsEnvironment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Main {
    private Main() {}

    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                launchDesktop(null);
                return;
            }
            if (args[0].equals("--open")) {
                if (args.length < 2 || args[1].isBlank() || args[1].startsWith("--"))
                    throw new IllegalArgumentException("--open 格式: --open /path/to/snapshot");
                var mappings = new ArrayList<PathMapping>();
                var jars = new ArrayList<Path>();
                for (int i = 2; i < args.length; i += 2) {
                    if (i + 1 == args.length) throw new IllegalArgumentException("缺少桌面启动选项参数: " + args[i]);
                    if (args[i].equals("--map")) mappings.add(pathMapping(args[i + 1]));
                    else if (args[i].equals("--jar")) jars.add(Path.of(args[i + 1]));
                    else throw new IllegalArgumentException("桌面启动仅支持 --map 和 --jar: " + args[i]);
                }
                launchDesktop(Path.of(args[1]), mappings, jars);
                return;
            }
            if (args.length == 1 && !args[0].startsWith("--")) {
                launchDesktop(Path.of(args[0]));
                return;
            }
            if (List.of(args).contains("--help")) { usage(); return; }
            if (args.length == 2 && args[0].equals("--create-example")) {
                dev.flinkdebug.demo.ExampleSnapshot.create(Path.of(args[1]));
                System.out.println("已生成真实 Flink savepoint: " + Path.of(args[1]).toAbsolutePath());
                return;
            }
            runCli(args);
        } catch (Exception e) {
            System.err.println("解析失败: " + e.getMessage());
            System.exit(2);
        }
    }

    private static void launchDesktop(Path snapshot) {
        launchDesktop(snapshot, List.of(), List.of());
    }

    private static void launchDesktop(Path snapshot, List<PathMapping> mappings, List<Path> jars) {
        if (GraphicsEnvironment.isHeadless()) throw new IllegalArgumentException("当前环境无桌面，可使用 --inspect、--preview 或 --query。");
        SwingUtilities.invokeLater(() -> {
            try {
                for (var theme : UIManager.getInstalledLookAndFeels()) {
                    if (theme.getName().equals("Nimbus")) UIManager.setLookAndFeel(theme.getClassName());
                }
            } catch (Exception ignored) { /* System theme is sufficient. */ }
            var frame = new StateLensFrame();
            frame.setVisible(true);
            if (snapshot != null) frame.openSnapshot(snapshot, mappings, jars);
        });
    }

    private static void runCli(String[] args) throws Exception {
        var options = new LinkedHashMap<String, String>();
        var mappings = new ArrayList<PathMapping>();
        var jars = new ArrayList<Path>();
        var allowed = List.of("--inspect", "--preview", "--query", "--operator", "--subtask", "--limit", "--state", "--kind", "--key-type", "--value-type", "--map-key-type", "--map", "--jar");
        for (int i = 0; i < args.length; i += 2) {
            String name = args[i];
            if (!allowed.contains(name) || i + 1 == args.length) throw new IllegalArgumentException("未知选项或缺少参数: " + name);
            String value = args[i + 1];
            if (name.equals("--map")) {
                mappings.add(pathMapping(value));
            } else if (name.equals("--jar")) jars.add(Path.of(value));
            else if (options.put(name, value) != null) throw new IllegalArgumentException("重复选项: " + name);
        }
        var modes = List.of("--inspect", "--preview", "--query").stream().filter(options::containsKey).toList();
        if (modes.size() != 1) throw new IllegalArgumentException("请指定一个 --inspect、--preview 或 --query。");
        String mode = modes.get(0);
        try (var session = new SnapshotInspector().open(Path.of(options.get(mode)), mappings, jars)) {
            Object output;
            if (mode.equals("--inspect")) output = session.report();
            else if (mode.equals("--preview")) output = new DataPreviewService().preview(session, required(options, "--operator"),
                    Integer.parseInt(options.getOrDefault("--subtask", "0")), Integer.parseInt(options.getOrDefault("--limit", "200")));
            else output = new LocalStreamQueryService().query(session, new LocalStreamQueryService.QuerySpec(
                        required(options, "--operator"), required(options, "--state"),
                        LocalStreamQueryService.Kind.valueOf(options.getOrDefault("--kind", "VALUE").toUpperCase(java.util.Locale.ROOT)),
                        scalar(options, "--key-type", "STRING"), scalar(options, "--value-type", "LONG"), scalar(options, "--map-key-type", "STRING"),
                        Integer.parseInt(options.getOrDefault("--limit", "200"))));
            System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(output));
        }
    }

    private static PathMapping pathMapping(String value) {
        int split = value.indexOf('=');
        if (split < 1 || split == value.length() - 1) throw new IllegalArgumentException("--map 格式: 原URI前缀=/本地目录");
        return new PathMapping(value.substring(0, split), Path.of(value.substring(split + 1)));
    }

    private static String required(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("缺少参数: " + key);
        return value;
    }

    private static LocalStreamQueryService.ScalarType scalar(Map<String, String> options, String key, String fallback) {
        return LocalStreamQueryService.ScalarType.valueOf(options.getOrDefault(key, fallback).toUpperCase(java.util.Locale.ROOT));
    }

    private static void usage() {
        System.out.println("""
                Flink State Lens · Flink %s 离线状态分析
                无参数: 打开桌面界面
                --open /path/to/snapshot: 打开桌面界面并自动导入快照（也可直接传入快照路径）
                    可同时指定 --map 和 --jar，预先设置本地路径映射和原作业类型
                --inspect /path/to/snapshot: 元数据与状态文件完整性 JSON
                --preview /path/to/snapshot --operator HASH [--subtask 0] [--limit 200]: 原始状态与可解码样本 JSON
                --query /path/to/snapshot --operator HASH --state NAME [--kind VALUE|LIST|MAP]
                    [--key-type STRING|INT|LONG|DOUBLE|BOOLEAN] [--value-type LONG] [--map-key-type STRING] [--limit 200]
                    通过本地 DataStream BATCH 读取整个算子的标准类型/default namespace 状态
                --map 's3://bucket/job=/local/job': 可重复指定原路径到本地目录的映射
                --jar /path/to/job.jar: 可重复添加原作业/依赖 JAR
                --create-example /new/output/directory: 使用 Flink 生成可分析的示例 savepoint（目录必须尚不存在）
                样本限制 1–10000；checkpoint 须同时下载全部 shared/private/meta 引用文件。
                """.formatted(EnvironmentInformation.getVersion()));
    }
}
