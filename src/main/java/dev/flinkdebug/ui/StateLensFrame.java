package dev.flinkdebug.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.flinkdebug.core.DataPreviewService;
import dev.flinkdebug.core.LocalStreamQueryService;
import dev.flinkdebug.core.PathMapping;
import dev.flinkdebug.core.PreviewReport;
import dev.flinkdebug.core.LocalStreamQueryService.QueryReport;
import dev.flinkdebug.core.SnapshotInspector;
import dev.flinkdebug.core.SnapshotReport;
import dev.flinkdebug.core.SnapshotSession;
import org.apache.flink.runtime.util.EnvironmentInformation;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableRowSorter;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreePath;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/** Local, read-only inspection UI. Exactly one background operation owns a session at a time. */
public final class StateLensFrame extends JFrame {
    private static final Color PAGE = new Color(239, 243, 249);
    private static final Color INK = new Color(30, 45, 67);
    private static final Color MUTED = new Color(99, 115, 139);
    private static final Color ACCENT = new Color(34, 91, 167);
    private static final String APP = "Flink State Lens";

    private final JTextField snapshotPath = new JTextField();
    private final JButton browseSnapshot = new JButton("选择快照…");
    private final JButton openSnapshot = new JButton("导入并分析");
    private final DefaultTableModel mappingModel = new DefaultTableModel(new String[]{"原始 URI 前缀", "下载后的本地目录"}, 0);
    private final JTable mappings = new JTable(mappingModel);
    private final JButton addMapping = new JButton("添加映射…");
    private final JButton removeMapping = new JButton("删除映射");
    private final JButton chooseJars = new JButton("选择用户 JAR…");
    private final JButton clearJars = new JButton("清除");
    private final JLabel jarSummary = new JLabel("未加载用户 JAR");
    private final List<Path> userJars = new ArrayList<>();

    private final JLabel snapshotMetric = metricValue();
    private final JLabel snapshotTypeSummary = new JLabel("类型：尚未加载");
    private final JLabel operatorMetric = metricValue();
    private final JLabel referencedMetric = metricValue();
    private final JLabel checkpointedMetric = metricValue();
    private final JLabel metadataSummary = new JLabel("导入本地快照后显示真实元数据");
    private final JTree operatorTree = new JTree(new DefaultMutableTreeNode("尚未导入快照"));
    private final JLabel selectionSummary = new JLabel("选择算子或子任务");
    private final DefaultTableModel filesModel = readOnlyModel("算子 ID", "子任务", "类别", "句柄类型", "Key-group", "本地状态", "声明大小", "实际大小", "原始路径", "本地路径");
    private final DefaultTableModel schemasModel = readOnlyModel("状态名", "状态类型", "Key 序列化器", "Namespace 序列化器", "Value 序列化器");
    private final DefaultTableModel samplesModel = readOnlyModel("状态名", "Key（解码）", "Map key（解码）", "Value（解码）", "Namespace（解码）", "TTL 时间戳（ms）", "Key-group", "解码结果", "Key 原始 HEX", "Value 原始 HEX");
    private final DefaultTableModel diagnosticsModel = readOnlyModel("级别", "诊断说明");
    private final JTable filesTable = table(filesModel);
    private final JTable schemasTable = table(schemasModel);
    private final JTable samplesTable = table(samplesModel);
    private final JTable diagnosticsTable = table(diagnosticsModel);
    private final TableRowSorter<DefaultTableModel> sampleSorter = new TableRowSorter<>(samplesModel);
    private final JTextField sampleSearch = new JTextField();
    private final JLabel schemaSummary = new JLabel("请选择具体子任务，然后点击“读取样本”加载状态 Schema。");
    private final JLabel sampleSummary = new JLabel("未读取状态数据。请选择具体子任务，然后点击“读取样本”。");
    private final JTabbedPane details = new JTabbedPane();
    private final JTextField queryStateName = new JTextField();
    private final JComboBox<LocalStreamQueryService.Kind> queryKind = new JComboBox<>(LocalStreamQueryService.Kind.values());
    private final JComboBox<LocalStreamQueryService.ScalarType> queryKeyType = scalarTypes();
    private final JComboBox<LocalStreamQueryService.ScalarType> queryValueType = scalarTypes();
    private final JComboBox<LocalStreamQueryService.ScalarType> queryMapKeyType = scalarTypes();
    private final JSpinner queryLimit = new JSpinner(new SpinnerNumberModel(100, 1, 10_000, 100));
    private final JButton runQuery = new JButton("本地解析");
    private final DefaultTableModel queryModel = readOnlyModel("Key", "Value");
    private final JTable queryTable = table(queryModel);
    private final JLabel querySummary = new JLabel("配置状态名和类型后，执行真实 Flink 本地有界作业。");
    private final JSpinner limit = new JSpinner(new SpinnerNumberModel(100, 1, 1_000, 100));
    private final JButton readSamples = new JButton("读取样本");
    private final JButton cancelOperation = new JButton("取消读取");
    private final JButton exportReport = new JButton("导出报告 JSON…");
    private final JButton exportSamples = new JButton("导出样本 CSV…");
    private final JLabel status = new JLabel("就绪 · 完全离线，仅分析本地副本");
    private final JProgressBar progress = new JProgressBar();

    private SnapshotSession session;
    private SnapshotReport report;
    private PreviewReport preview;
    private List<Path> loadedJars = List.of();
    private TreeItem selected;
    private long generation;
    private boolean busy;
    private boolean closing;
    private boolean cancellable;
    private volatile boolean cancellationRequested;
    private final Object operationLock = new Object();
    private Thread operationThread;

    public StateLensFrame() {
        super(APP + " — 离线 Flink 状态分析");
        installTheme();
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        setMinimumSize(new Dimension(1000, 720));
        setSize(1280, 900);
        setLocationRelativeTo(null);
        queryValueType.setSelectedItem(LocalStreamQueryService.ScalarType.LONG);
        setContentPane(buildContent());
        SwingUtilities.updateComponentTreeUI(this);
        wireActions();
        updateActions();
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent e) { requestClose(); }
        });
    }

    /** Starts the same asynchronous import as the UI button; call on the Swing event thread. */
    public void openSnapshot(Path path) {
        requireImportReady();
        snapshotPath.setText(Objects.requireNonNull(path, "快照路径不能为空。").toAbsolutePath().normalize().toString());
        loadSnapshot();
    }

    /** Initializes relocation/JAR settings before starting the regular asynchronous import. */
    public void openSnapshot(Path path, List<PathMapping> relocation, List<Path> jars) {
        requireImportReady();
        Path normalizedSnapshot = Objects.requireNonNull(path, "快照路径不能为空。").toAbsolutePath().normalize();
        List<PathMapping> mappingCopy = List.copyOf(Objects.requireNonNull(relocation, "路径映射列表不能为空。"));
        List<Path> jarCopy = List.copyOf(Objects.requireNonNull(jars, "用户 JAR 列表不能为空。"));
        List<PathMapping> normalizedMappings = new ArrayList<>(mappingCopy.size());
        for (PathMapping mapping : mappingCopy) {
            String prefix = Objects.requireNonNull(mapping.originalPrefix(), "映射的原始 URI 前缀不能为空。").trim();
            if (prefix.isBlank()) throw new IllegalArgumentException("映射的原始 URI 前缀不能为空。");
            Path directory = Objects.requireNonNull(mapping.localDirectory(), "映射的本地目录不能为空。").toAbsolutePath().normalize();
            normalizedMappings.add(new PathMapping(prefix, directory));
        }
        List<Path> normalizedJars = new ArrayList<>(jarCopy.size());
        for (Path jar : jarCopy) {
            Path normalizedJar = jar.toAbsolutePath().normalize();
            if (!normalizedJars.contains(normalizedJar)) normalizedJars.add(normalizedJar);
        }

        // Validate/copy all arguments above before replacing any visible import configuration.
        if (mappings.isEditing()) mappings.getCellEditor().cancelCellEditing();
        mappingModel.setRowCount(0);
        for (PathMapping mapping : normalizedMappings) {
            mappingModel.addRow(new Object[]{mapping.originalPrefix(), mapping.localDirectory().toString()});
        }
        userJars.clear();
        userJars.addAll(normalizedJars);
        snapshotPath.setText(normalizedSnapshot.toString());
        refreshJars();
        loadSnapshot();
    }

    private void requireImportReady() {
        if (!SwingUtilities.isEventDispatchThread()) {
            throw new IllegalStateException("快照导入必须在 Swing 事件线程上启动。");
        }
        if (busy || closing) throw new IllegalStateException("当前任务尚未结束，无法启动新的快照导入。");
    }

    private static void installTheme() {
        try {
            for (UIManager.LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (Exception ignored) { /* The platform look and feel is a valid fallback. */ }
        UIManager.put("nimbusBase", new Color(48, 75, 113));
        UIManager.put("nimbusSelectionBackground", ACCENT);
        UIManager.put("control", PAGE);
    }

    private JPanel buildContent() {
        JPanel content = new JPanel(new BorderLayout(0, 12));
        content.setBackground(PAGE);
        content.setBorder(new EmptyBorder(16, 20, 12, 20));
        JPanel top = new JPanel();
        top.setOpaque(false);
        top.setLayout(new BoxLayout(top, BoxLayout.Y_AXIS));
        top.add(header());
        top.add(Box.createVerticalStrut(12));
        top.add(importPanel());
        top.add(Box.createVerticalStrut(12));
        top.add(metricsPanel());
        content.add(top, BorderLayout.NORTH);
        content.add(workspace(), BorderLayout.CENTER);
        content.add(footer(), BorderLayout.SOUTH);
        return content;
    }

    private JPanel header() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setOpaque(false);
        JLabel title = new JLabel(APP);
        title.setFont(title.getFont().deriveFont(Font.BOLD, 25f));
        title.setForeground(INK);
        JLabel subtitle = new JLabel("离线快照分析  /  Flink " + EnvironmentInformation.getVersion() + "  /  RocksDB");
        subtitle.setForeground(MUTED);
        panel.add(title, BorderLayout.WEST);
        panel.add(subtitle, BorderLayout.EAST);
        return panel;
    }

    private JPanel importPanel() {
        JPanel card = card();
        card.setLayout(new BorderLayout(8, 6));
        JPanel source = new JPanel(new BorderLayout(8, 0));
        source.setOpaque(false);
        source.add(new JLabel("本地快照"), BorderLayout.WEST);
        snapshotPath.setToolTipText("选择 checkpoint/savepoint 目录或 _metadata 文件；请先手动下载所有引用的状态文件。");
        source.add(snapshotPath, BorderLayout.CENTER);
        JPanel actions = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        actions.setOpaque(false);
        actions.add(browseSnapshot);
        actions.add(openSnapshot);
        source.add(actions, BorderLayout.EAST);
        card.add(source, BorderLayout.NORTH);

        JPanel relocation = new JPanel(new BorderLayout(8, 4));
        relocation.setOpaque(false);
        JLabel help = new JLabel("路径映射：将元数据中的原始 URI 前缀替换为本地下载目录；相对路径可直接解析。");
        help.setForeground(MUTED);
        relocation.add(help, BorderLayout.NORTH);
        mappings.setRowHeight(25);
        mappings.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        mappings.setToolTipText("示例：hdfs://cluster/flink/shared/ → 下载目录中的 shared 文件夹。可配置多条；加载前生效。");
        JScrollPane mapScroll = new JScrollPane(mappings);
        mapScroll.setPreferredSize(new Dimension(600, 70));
        relocation.add(mapScroll, BorderLayout.CENTER);
        JPanel mapButtons = new JPanel();
        mapButtons.setOpaque(false);
        mapButtons.setLayout(new BoxLayout(mapButtons, BoxLayout.Y_AXIS));
        mapButtons.add(addMapping);
        mapButtons.add(Box.createVerticalStrut(5));
        mapButtons.add(removeMapping);
        relocation.add(mapButtons, BorderLayout.EAST);
        card.add(relocation, BorderLayout.CENTER);

        JPanel jars = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        jars.setOpaque(false);
        jars.add(chooseJars);
        jars.add(clearJars);
        jars.add(jarSummary);
        JLabel jarHelp = new JLabel("自定义类型的解码通常需要原作业 JAR；原始 HEX 始终与解码列分开显示。");
        jarHelp.setForeground(MUTED);
        jars.add(jarHelp);
        card.add(jars, BorderLayout.SOUTH);
        return card;
    }

    private JPanel metricsPanel() {
        JPanel panel = new JPanel(new BorderLayout(0, 7));
        panel.setOpaque(false);
        JPanel cards = new JPanel(new GridLayout(1, 4, 12, 0));
        cards.setOpaque(false);
        JPanel snapshotCard = metricCard("Checkpoint ID", snapshotMetric);
        snapshotTypeSummary.setForeground(MUTED);
        snapshotCard.add(snapshotTypeSummary, BorderLayout.SOUTH);
        cards.add(snapshotCard);
        cards.add(metricCard("算子数", operatorMetric));
        cards.add(metricCard("引用状态大小", referencedMetric));
        cards.add(metricCard("本次持久化大小", checkpointedMetric));
        panel.add(cards, BorderLayout.CENTER);
        metadataSummary.setForeground(MUTED);
        panel.add(metadataSummary, BorderLayout.SOUTH);
        return panel;
    }

    private JComponent workspace() {
        JPanel navigation = card();
        navigation.setLayout(new BorderLayout(0, 8));
        navigation.add(new JLabel("算子 / 子任务（并行实例）"), BorderLayout.NORTH);
        operatorTree.setRootVisible(true);
        operatorTree.setShowsRootHandles(true);
        operatorTree.setRowHeight(28);
        operatorTree.setCellRenderer(new DefaultTreeCellRenderer() {
            @Override public Component getTreeCellRendererComponent(JTree tree, Object value, boolean selected,
                                                                     boolean expanded, boolean leaf, int row,
                                                                     boolean hasFocus) {
                Component component = super.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, hasFocus);
                setToolTipText(value instanceof DefaultMutableTreeNode node && node.getUserObject() instanceof TreeItem item
                        ? item.identityTooltip() : null);
                return component;
            }
        });
        ToolTipManager.sharedInstance().registerComponent(operatorTree);
        navigation.add(new JScrollPane(operatorTree), BorderLayout.CENTER);
        selectionSummary.setForeground(MUTED);
        navigation.add(selectionSummary, BorderLayout.SOUTH);

        filesTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        for (int i = 0; i < filesTable.getColumnCount(); i++) {
            filesTable.getColumnModel().getColumn(i).setPreferredWidth(i >= 8 ? 320 : i == 0 ? 290 : i == 3 ? 200 : 115);
        }
        details.addTab("状态文件", new JScrollPane(filesTable));
        schemasTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        for (int i = 0; i < schemasTable.getColumnCount(); i++) schemasTable.getColumnModel().getColumn(i).setPreferredWidth(i < 2 ? 170 : 300);
        details.addTab("状态 Schema", schemasPanel());
        details.addTab("数据样本", samplesPanel());
        details.addTab("业务状态查询", queryPanel());
        diagnosticsTable.getColumnModel().getColumn(0).setPreferredWidth(80);
        diagnosticsTable.getColumnModel().getColumn(0).setMaxWidth(120);
        details.addTab("诊断", new JScrollPane(diagnosticsTable));

        JPanel inspector = new JPanel(new BorderLayout());
        inspector.setOpaque(false);
        inspector.add(details, BorderLayout.CENTER);
        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, navigation, inspector);
        split.setBorder(null);
        split.setResizeWeight(0.21);
        split.setDividerLocation(250);
        split.setContinuousLayout(true);
        return split;
    }

    private JPanel schemasPanel() {
        JPanel panel = new JPanel(new BorderLayout(8, 8));
        panel.setBorder(new EmptyBorder(8, 8, 8, 8));
        schemaSummary.setForeground(MUTED);
        panel.add(schemaSummary, BorderLayout.NORTH);
        panel.add(new JScrollPane(schemasTable), BorderLayout.CENTER);
        return panel;
    }

    private JPanel samplesPanel() {
        JPanel panel = new JPanel(new BorderLayout(8, 8));
        panel.setBorder(new EmptyBorder(8, 8, 8, 8));
        JPanel search = new JPanel(new BorderLayout(8, 0));
        search.add(new JLabel("搜索当前样本"), BorderLayout.WEST);
        search.add(sampleSearch, BorderLayout.CENTER);
        panel.add(search, BorderLayout.NORTH);
        samplesTable.setRowSorter(sampleSorter);
        samplesTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        int[] sampleWidths = {175, 165, 170, 310, 105, 145, 75, 250, 265, 265};
        for (int i = 0; i < sampleWidths.length; i++) samplesTable.getColumnModel().getColumn(i).setPreferredWidth(sampleWidths[i]);
        panel.add(new JScrollPane(samplesTable), BorderLayout.CENTER);
        sampleSummary.setForeground(MUTED);
        panel.add(sampleSummary, BorderLayout.SOUTH);
        return panel;
    }

    private JPanel queryPanel() {
        JPanel panel = new JPanel(new BorderLayout(8, 8));
        panel.setBorder(new EmptyBorder(8, 8, 8, 8));
        JPanel configuration = new JPanel(new BorderLayout(0, 7));
        JLabel scope = new JLabel("读取所选算子的全部子任务 · 标准基础类型 · 默认 Namespace · 在本机执行 Flink BATCH 作业");
        scope.setForeground(MUTED);
        configuration.add(scope, BorderLayout.NORTH);
        JPanel fields = new JPanel(new GridLayout(2, 5, 8, 5));
        fields.add(new JLabel("状态名"));
        fields.add(new JLabel("状态类型"));
        fields.add(new JLabel("Key 类型"));
        fields.add(new JLabel("Value 类型"));
        fields.add(new JLabel("Map Key 类型"));
        fields.add(queryStateName);
        fields.add(queryKind);
        fields.add(queryKeyType);
        fields.add(queryValueType);
        fields.add(queryMapKeyType);
        configuration.add(fields, BorderLayout.CENTER);
        JPanel action = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        action.add(new JLabel("返回上限"));
        queryLimit.setPreferredSize(new Dimension(95, 28));
        action.add(queryLimit);
        action.add(runQuery);
        configuration.add(action, BorderLayout.SOUTH);
        panel.add(configuration, BorderLayout.NORTH);
        panel.add(new JScrollPane(queryTable), BorderLayout.CENTER);
        querySummary.setForeground(MUTED);
        panel.add(querySummary, BorderLayout.SOUTH);
        return panel;
    }

    private JPanel footer() {
        JPanel footer = new JPanel(new BorderLayout(0, 8));
        footer.setOpaque(false);
        JPanel controls = new JPanel(new BorderLayout());
        controls.setOpaque(false);
        JPanel previewButtons = new JPanel(new FlowLayout(FlowLayout.LEFT, 7, 0));
        previewButtons.setOpaque(false);
        previewButtons.add(new JLabel("样本上限"));
        limit.setPreferredSize(new Dimension(95, 28));
        limit.setToolTipText("每次只返回有限原始样本，最多 1,000 条；样本数不代表完整状态记录数。");
        previewButtons.add(limit);
        previewButtons.add(readSamples);
        previewButtons.add(cancelOperation);
        controls.add(previewButtons, BorderLayout.WEST);
        JPanel exports = new JPanel(new FlowLayout(FlowLayout.RIGHT, 7, 0));
        exports.setOpaque(false);
        exports.add(exportReport);
        exports.add(exportSamples);
        exportSamples.setToolTipText("导出本次读取的全部样本，包含原始 HEX 和解码状态；搜索过滤不影响导出。");
        controls.add(exports, BorderLayout.EAST);
        footer.add(controls, BorderLayout.NORTH);
        JPanel statusRow = new JPanel(new BorderLayout(10, 0));
        statusRow.setOpaque(false);
        status.setForeground(MUTED);
        statusRow.add(status, BorderLayout.CENTER);
        progress.setPreferredSize(new Dimension(160, 8));
        progress.setVisible(false);
        statusRow.add(progress, BorderLayout.EAST);
        footer.add(statusRow, BorderLayout.SOUTH);
        return footer;
    }

    private void wireActions() {
        browseSnapshot.addActionListener(e -> selectSnapshot());
        openSnapshot.addActionListener(e -> loadSnapshot());
        snapshotPath.addActionListener(e -> loadSnapshot());
        addMapping.addActionListener(e -> addPathMapping());
        removeMapping.addActionListener(e -> {
            int row = mappings.getSelectedRow();
            if (row >= 0) mappingModel.removeRow(mappings.convertRowIndexToModel(row));
            updateActions();
        });
        chooseJars.addActionListener(e -> selectJars());
        clearJars.addActionListener(e -> { userJars.clear(); refreshJars(); });
        operatorTree.addTreeSelectionListener(e -> selectTreeNode());
        readSamples.addActionListener(e -> loadPreview());
        cancelOperation.addActionListener(e -> cancelReading());
        runQuery.addActionListener(e -> loadQuery());
        queryKind.addActionListener(e -> updateActions());
        schemasTable.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting() && !busy && !closing) useSelectedSchema();
        });
        exportReport.addActionListener(e -> saveReport());
        exportSamples.addActionListener(e -> saveSamples());
        sampleSearch.getDocument().addDocumentListener(new DocumentListener() {
            @Override public void insertUpdate(DocumentEvent e) { filterSamples(); }
            @Override public void removeUpdate(DocumentEvent e) { filterSamples(); }
            @Override public void changedUpdate(DocumentEvent e) { filterSamples(); }
        });
    }

    private void selectSnapshot() {
        JFileChooser chooser = chooser("选择 checkpoint/savepoint 目录或 _metadata 文件");
        chooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        if (chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) snapshotPath.setText(chooser.getSelectedFile().getAbsolutePath());
    }

    private void addPathMapping() {
        JTextField prefix = new JTextField(32);
        JTextField local = new JTextField(32);
        JButton browse = new JButton("选择目录…");
        browse.addActionListener(e -> {
            JFileChooser chooser = chooser("选择已下载的本地状态目录");
            chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
            if (chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) local.setText(chooser.getSelectedFile().getAbsolutePath());
        });
        JPanel fields = new JPanel(new GridLayout(0, 1, 4, 4));
        fields.add(new JLabel("原始 URI 前缀（例如 hdfs://cluster/flink/shared/）"));
        fields.add(prefix);
        fields.add(new JLabel("本地目录（必须对应上述前缀的位置）"));
        JPanel pathRow = new JPanel(new BorderLayout(5, 0));
        pathRow.add(local, BorderLayout.CENTER);
        pathRow.add(browse, BorderLayout.EAST);
        fields.add(pathRow);
        if (JOptionPane.showConfirmDialog(this, fields, "添加路径映射", JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE) != JOptionPane.OK_OPTION) return;
        if (prefix.getText().isBlank() || local.getText().isBlank()) {
            showFailure("路径映射不完整", new IllegalArgumentException("原始 URI 前缀和本地目录不能为空。"));
            return;
        }
        mappingModel.addRow(new Object[]{prefix.getText().trim(), local.getText().trim()});
        updateActions();
    }

    private void selectJars() {
        JFileChooser chooser = chooser("选择原作业及其依赖 JAR（支持多选）");
        chooser.setMultiSelectionEnabled(true);
        chooser.setFileFilter(new javax.swing.filechooser.FileNameExtensionFilter("Java JAR 文件", "jar"));
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        for (java.io.File file : chooser.getSelectedFiles()) {
            Path path = file.toPath().toAbsolutePath().normalize();
            if (!userJars.contains(path)) userJars.add(path);
        }
        refreshJars();
    }

    private void refreshJars() {
        jarSummary.setText(userJars.isEmpty() ? "未加载用户 JAR" : "已选择 " + userJars.size() + " 个 JAR（下次导入生效）");
        jarSummary.setToolTipText(userJars.isEmpty() ? null : userJars.toString());
        updateActions();
    }

    private List<PathMapping> pathMappings() {
        if (mappings.isEditing() && !mappings.getCellEditor().stopCellEditing()) throw new IllegalArgumentException("请先完成路径映射编辑。");
        List<PathMapping> result = new ArrayList<>();
        for (int row = 0; row < mappingModel.getRowCount(); row++) {
            String prefix = Objects.toString(mappingModel.getValueAt(row, 0), "").trim();
            String local = Objects.toString(mappingModel.getValueAt(row, 1), "").trim();
            if (prefix.isBlank() || local.isBlank()) throw new IllegalArgumentException("第 " + (row + 1) + " 条路径映射不能为空。");
            Path directory = Path.of(local).toAbsolutePath().normalize();
            if (!Files.isDirectory(directory)) throw new IllegalArgumentException("映射目录不存在：" + directory);
            result.add(new PathMapping(prefix, directory));
        }
        return List.copyOf(result);
    }

    private void loadSnapshot() {
        if (busy || closing) return;
        final Path source;
        final List<PathMapping> relocation;
        try {
            if (snapshotPath.getText().isBlank()) throw new IllegalArgumentException("请选择本地快照目录或 _metadata 文件。");
            source = Path.of(snapshotPath.getText().trim()).toAbsolutePath().normalize();
            relocation = pathMappings();
        } catch (Exception error) {
            logImportFailure(error);
            showFailure("无法导入", error);
            return;
        }
        List<Path> jars = List.copyOf(userJars);
        SnapshotSession previous = session;
        long request = ++generation;
        beginOperation("正在读取快照元数据…");
        System.out.println("[Flink State Lens] snapshot import started");
        new SwingWorker<SnapshotSession, Void>() {
            @Override protected SnapshotSession doInBackground() throws Exception {
                SnapshotSession opened = new SnapshotInspector().open(source, relocation, jars);
                try { if (previous != null) previous.close(); }
                catch (Exception error) {
                    try { opened.close(); } catch (Exception suppressed) { error.addSuppressed(suppressed); }
                    throw error;
                }
                return opened;
            }
            @Override protected void done() {
                try {
                    SnapshotSession opened = get();
                    if (request != generation) { closeSessionAsync(opened); return; }
                    session = opened;
                    loadedJars = jars;
                    report = opened.report();
                    preview = null;
                    renderReport();
                    setStatus(report.diagnostics().isEmpty() ? "快照已加载 · 请选择子任务读取有界样本" : "快照已加载 · " + report.diagnostics().size() + " 条诊断，详见“诊断”页", hasError(report) ? "ERROR" : report.diagnostics().isEmpty() ? "INFO" : "WARN");
                    System.out.println("[Flink State Lens] snapshot import completed: checkpointId="
                            + report.checkpointId() + ", operators=" + report.operators().size()
                            + ", files=" + report.files().size() + ", diagnostics=" + report.diagnostics().size());
                } catch (Exception error) {
                    Throwable cause = unwrap(error);
                    logImportFailure(cause);
                    showFailure("导入失败", cause);
                }
                finally { endOperation(); }
            }
        }.execute();
    }

    private static void logImportFailure(Throwable error) {
        String reason = Objects.toString(error.getMessage(), error.getClass().getSimpleName()).replace('\n', ' ').replace('\r', ' ');
        if (reason.length() > 300) reason = reason.substring(0, 300) + "…";
        System.err.println("[Flink State Lens] snapshot import failed: " + error.getClass().getSimpleName() + " - " + reason);
    }

    private void renderReport() {
        snapshotMetric.setText(Long.toString(report.checkpointId()));
        snapshotTypeSummary.setText("类型：" + shortSnapshotKind(report.snapshotKind()));
        snapshotTypeSummary.setToolTipText(report.snapshotKind());
        operatorMetric.setText(Integer.toString(report.operators().size()));
        referencedMetric.setText(bytes(report.referencedStateBytes()));
        checkpointedMetric.setText(bytes(report.checkpointedBytes()));
        metadataSummary.setText("运行时 " + report.runtimeVersion() + "   ·   元数据格式 v" + report.metadataVersion() + "   ·   " + report.metadataPath());
        metadataSummary.setToolTipText(report.metadataPath().toString());
        diagnosticsModel.setRowCount(0);
        report.diagnostics().forEach(d -> diagnosticsModel.addRow(new Object[]{d.severity(), d.message()}));
        DefaultMutableTreeNode root = new DefaultMutableTreeNode("快照 " + report.checkpointId());
        for (SnapshotReport.OperatorReport operator : report.operators()) {
            String identity = operatorIdentity(operator);
            DefaultMutableTreeNode node = new DefaultMutableTreeNode(new TreeItem(operator.operatorId(), -1,
                    operatorLabel(operator), identity));
            for (SnapshotReport.SubtaskReport subtask : operator.subtasks()) {
                String instance = "子任务 " + subtask.index() + "/" + operator.parallelism();
                node.add(new DefaultMutableTreeNode(new TreeItem(operator.operatorId(), subtask.index(),
                        instance + " · " + bytes(subtask.referencedStateBytes()),
                        identity.replace("</html>", "<br>" + instance + "（并行实例序号 / 并行度）</html>"))));
            }
            root.add(node);
        }
        operatorTree.setModel(new DefaultTreeModel(root));
        operatorTree.expandRow(0);
        operatorTree.setSelectionRow(0);
        selected = null;
        selectionSummary.setText("全部状态文件");
        selectionSummary.setToolTipText(null);
        renderFiles();
        clearPreview();
        clearQuery();
    }

    private void selectTreeNode() {
        if (busy || closing) return;
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) operatorTree.getLastSelectedPathComponent();
        TreeItem next = node != null && node.getUserObject() instanceof TreeItem item ? item : null;
        if (Objects.equals(selected, next)) return;
        selected = next;
        if (selected != null && selected.subtask() < 0) operatorTree.expandPath(new TreePath(node.getPath()));
        selectionSummary.setText(selected == null ? "全部状态文件" : selected.subtask() < 0 ? "选择子任务以读取数据" : "子任务 " + selected.subtask());
        selectionSummary.setToolTipText(selected == null ? null : selected.identityTooltip());
        if (selected != null && report != null) {
            report.operators().stream().filter(op -> op.operatorId().equals(selected.operatorId())).findFirst().ifPresent(op -> {
                selectionSummary.setText("<html>名称：" + html(shortText(identityField(op.operatorName()), 24))
                        + "<br>UID：" + html(shortText(identityField(op.operatorUid()), 24))
                        + "<br>Hash：" + html(shortHash(op.operatorId()))
                        + "<br>并行度 " + op.parallelism() + " / 最大 " + op.maxParallelism() + "<br>"
                        + (selected.subtask() < 0 ? "选择子任务以读取原始数据" : "已选子任务 " + selected.subtask() + "/" + op.parallelism() + "（并行实例）")
                        + (op.fullyFinished() ? "<br>算子已全部完成" : "") + "</html>");
            });
        }
        renderFiles();
        clearPreview();
        clearQuery();
        updateActions();
        setStatus(selected == null ? "未读取 · 请选择算子和具体子任务"
                : selected.subtask() < 0 ? "未读取 · 已展开算子，请选择具体子任务并点击“读取样本”"
                : "未读取 · 已选择子任务 " + selected.subtask() + "，点击“读取样本”加载 Schema 和数据", "INFO");
    }

    private void renderFiles() {
        filesModel.setRowCount(0);
        if (report == null) return;
        for (SnapshotReport.StateFile file : report.files()) {
            if (selected != null && (!selected.operatorId().equals(file.operatorId()) || selected.subtask() >= 0 && selected.subtask() != file.subtask())) continue;
            filesModel.addRow(new Object[]{file.operatorId(), file.subtask(), file.category(), file.handleType(), file.keyGroupRange(), availability(file.availability()), bytes(file.declaredBytes()), file.actualBytes() == null ? "—" : bytes(file.actualBytes()), file.logicalPath(), file.localPath()});
        }
        details.setTitleAt(0, "状态文件（" + filesModel.getRowCount() + "）");
    }

    private void clearPreview() {
        preview = null;
        schemasModel.setRowCount(0);
        samplesModel.setRowCount(0);
        String message = session == null ? "尚未加载快照。导入快照后，选择具体子任务并点击“读取样本”。"
                : selected == null ? "未读取：请选择算子，再选择具体子任务并点击“读取样本”。"
                : selected.subtask() < 0 ? "未读取：当前选择的是算子。请选择其下的具体子任务，再点击“读取样本”。"
                : "未读取：当前子任务的 Schema 和样本尚未加载。点击“读取样本”开始读取。";
        setPreviewMessage(message);
        details.setTitleAt(1, "状态 Schema");
        details.setTitleAt(2, "数据样本");
        if (report != null) {
            diagnosticsModel.setRowCount(0);
            report.diagnostics().forEach(d -> diagnosticsModel.addRow(new Object[]{d.severity(), d.message()}));
        }
    }

    private void setPreviewMessage(String message) {
        schemaSummary.setText(message);
        schemaSummary.setToolTipText(message);
        schemaSummary.setForeground(MUTED);
        sampleSummary.setText(message);
        sampleSummary.setToolTipText(message);
        sampleSummary.setForeground(MUTED);
    }

    private void previewCancelled() {
        clearPreview();
        setPreviewMessage("读取已取消，当前子任务的 Schema 和样本尚未加载。点击“读取样本”可重试。");
        setStatus("样本读取已取消 · 可点击“读取样本”重试", "INFO");
    }

    private void loadPreview() {
        if (busy || closing || session == null || selected == null || selected.subtask() < 0) return;
        try { limit.commitEdit(); }
        catch (java.text.ParseException error) { showFailure("样本上限无效", error); return; }
        SnapshotSession reading = session;
        TreeItem readingSelection = selected;
        int sampleLimit = (Integer) limit.getValue();
        long request = generation;
        setPreviewMessage("正在读取当前子任务的 Schema 和有界样本…" + (preview == null ? "" : " 表格暂时保留上次结果。"));
        beginOperation("正在读取 RocksDB 状态，最多 " + sampleLimit + " 条…", true);
        new SwingWorker<PreviewReport, Void>() {
            @Override protected PreviewReport doInBackground() throws Exception {
                return executeReading(() -> new DataPreviewService().preview(reading, readingSelection.operatorId(), readingSelection.subtask(), sampleLimit));
            }
            @Override protected void done() {
                try {
                    PreviewReport result = get();
                    if (cancellationRequested) { previewCancelled(); return; }
                    if (request != generation || reading != session || !readingSelection.equals(selected)) return;
                    preview = result;
                    renderPreview();
                    details.setSelectedIndex(2);
                    setStatus(result.warnings().isEmpty() ? result.entries().isEmpty()
                                    ? "读取完成 · 当前子任务未返回样本，可查看 Schema 或选择其他子任务"
                                    : "读取完成 · 已返回 " + result.entries().size() + " 条样本"
                            : "读取返回 " + result.entries().size() + " 条样本 · " + result.warnings().size() + " 条警告，请查看“诊断”页确认原因",
                            result.warnings().isEmpty() ? "INFO" : "WARN");
                } catch (Exception error) {
                    clearPreview();
                    if (cancellationRequested) previewCancelled();
                    else {
                        setPreviewMessage("读取失败，未加载当前子任务的 Schema 和样本。请查看“诊断”页中的错误原因并重试。");
                        schemaSummary.setForeground(new Color(174, 45, 55));
                        sampleSummary.setForeground(new Color(174, 45, 55));
                        showFailure("样本读取失败", unwrap(error));
                    }
                } finally { endOperation(); }
            }
        }.execute();
    }

    private void renderPreview() {
        schemasModel.setRowCount(0);
        samplesModel.setRowCount(0);
        for (PreviewReport.StateSchema schema : preview.schemas()) schemasModel.addRow(new Object[]{schema.name(), schema.type(), schema.keySerializer(), schema.namespaceSerializer(), schema.valueSerializer()});
        for (PreviewReport.StateEntry entry : preview.entries()) samplesModel.addRow(new Object[]{entry.stateName(), entry.key(), entry.mapKey(), entry.value(), entry.namespace(), entry.ttlTimestamp(), entry.keyGroup(), entry.decodeStatus(), entry.keyHex(), entry.valueHex()});
        diagnosticsModel.setRowCount(0);
        report.diagnostics().forEach(d -> diagnosticsModel.addRow(new Object[]{d.severity(), d.message()}));
        preview.warnings().forEach(w -> diagnosticsModel.addRow(new Object[]{"WARN", w}));
        details.setTitleAt(1, "状态 Schema（" + preview.schemas().size() + "）");
        details.setTitleAt(2, "数据样本（" + preview.entries().size() + "）");
        String warnings = preview.warnings().isEmpty() ? "" : " · " + preview.warnings().size() + " 条警告，请查看“诊断”页确认原因";
        schemaSummary.setText(preview.schemas().isEmpty()
                ? "本次未返回可展示的状态 Schema" + warnings + "；这不表示算子没有状态。"
                : "已读取当前子任务的 " + preview.schemas().size() + " 个状态 Schema" + warnings);
        schemaSummary.setToolTipText(schemaSummary.getText());
        long decoded = preview.entries().stream().filter(entry -> hasDecodeStatus(entry, "DECODED")).count();
        long partial = preview.entries().stream().filter(entry -> hasDecodeStatus(entry, "PARTIAL")).count();
        long raw = preview.entries().size() - decoded - partial;
        sampleSummary.setText("样本 " + preview.entries().size() + " 条 · 完整解码 " + decoded + " · 部分解码 " + partial
                + " · 原始/失败 " + raw + (preview.truncated() ? " · 已截断" : "")
                + "；样本不代表状态总记录数。TTL 为存储的访问时间戳，不判断过期。HEX 可横向滚动/双击查看。");
        sampleSummary.setToolTipText(sampleSummary.getText());
        if (preview.entries().isEmpty()) {
            sampleSummary.setText(preview.warnings().isEmpty()
                    ? "读取成功，当前子任务未返回样本；其他子任务需分别选择和读取。"
                    : "当前子任务未返回样本" + warnings + "；可能包含未支持的状态类型或读取失败，请以诊断为准。");
            sampleSummary.setToolTipText(sampleSummary.getText());
        }
        filterSamples();
    }

    private void clearQuery() {
        queryModel.setRowCount(0);
        querySummary.setText("配置状态名和类型后，执行真实 Flink 本地有界作业。");
    }

    private void loadQuery() {
        if (busy || closing || session == null || selected == null) return;
        if (queryStateName.getText().isBlank()) {
            showFailure("缺少状态名", new IllegalArgumentException("请输入作业中 StateDescriptor 使用的状态名。"));
            return;
        }
        try { queryLimit.commitEdit(); }
        catch (java.text.ParseException error) { showFailure("查询上限无效", error); return; }
        SnapshotSession reading = session;
        TreeItem readingSelection = selected;
        final LocalStreamQueryService.QuerySpec spec;
        try {
            spec = new LocalStreamQueryService.QuerySpec(
                    readingSelection.operatorId(), queryStateName.getText().trim(),
                    (LocalStreamQueryService.Kind) queryKind.getSelectedItem(),
                    (LocalStreamQueryService.ScalarType) queryKeyType.getSelectedItem(),
                    (LocalStreamQueryService.ScalarType) queryValueType.getSelectedItem(),
                    (LocalStreamQueryService.ScalarType) queryMapKeyType.getSelectedItem(),
                    (Integer) queryLimit.getValue());
        } catch (IllegalArgumentException error) { showFailure("查询配置无效", error); return; }
        long request = generation;
        beginOperation("正在本机执行状态读取作业 · 读取所选算子的全部子任务…", true);
        new SwingWorker<QueryReport, Void>() {
            @Override protected QueryReport doInBackground() throws Exception {
                return executeReading(() -> new LocalStreamQueryService().query(reading, spec));
            }
            @Override protected void done() {
                try {
                    QueryReport result = get();
                    if (cancellationRequested) { clearQuery(); setStatus("业务状态查询已取消", "INFO"); return; }
                    if (request != generation || reading != session || !readingSelection.equals(selected)) return;
                    queryModel.setRowCount(0);
                    result.rows().forEach(row -> queryModel.addRow(new Object[]{row.key(), row.value()}));
                    result.warnings().forEach(w -> diagnosticsModel.addRow(new Object[]{"WARN", w}));
                    querySummary.setText("返回 " + result.rows().size() + " 条业务状态" + (result.truncated() ? " · 已截断" : "") + " · 整个算子，所有子任务；结果数量不代表总记录数。");
                    setStatus("业务状态查询完成 · " + result.rows().size() + " 条" + (result.warnings().isEmpty() ? "" : "，请查看诊断警告"), result.warnings().isEmpty() ? "INFO" : "WARN");
                } catch (Exception error) {
                    clearQuery();
                    if (cancellationRequested) setStatus("业务状态查询已取消", "INFO");
                    else showFailure("业务状态查询失败", unwrap(error));
                } finally { endOperation(); }
            }
        }.execute();
    }

    private void filterSamples() {
        String query = sampleSearch.getText().trim();
        sampleSorter.setRowFilter(query.isEmpty() ? null : RowFilter.regexFilter("(?iu)" + Pattern.quote(query)));
    }

    private void useSelectedSchema() {
        int row = schemasTable.getSelectedRow();
        if (row < 0) return;
        row = schemasTable.convertRowIndexToModel(row);
        queryStateName.setText(Objects.toString(schemasModel.getValueAt(row, 0), ""));
        String type = Objects.toString(schemasModel.getValueAt(row, 1), "").toUpperCase(java.util.Locale.ROOT);
        for (LocalStreamQueryService.Kind kind : LocalStreamQueryService.Kind.values()) {
            if (type.contains(kind.name())) { queryKind.setSelectedItem(kind); break; }
        }
        querySummary.setText("已从 Schema 填入状态名；请核对 Key / Value 类型后执行本地解析。");
    }

    private void saveReport() {
        if (busy || report == null) return;
        Path target = exportTarget("导出本地分析报告", "flink-state-report.json", ".json");
        if (target == null) return;
        ReportExport document = new ReportExport(APP, report, selected == null ? null : selected.operatorId(), selected == null || selected.subtask() < 0 ? null : selected.subtask(), preview);
        runExport(target, () -> new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(target.toFile(), document));
    }

    private void saveSamples() {
        if (busy || preview == null || preview.entries().isEmpty()) return;
        Path target = exportTarget("导出当前子任务的全部已读取样本", "flink-state-samples.csv", ".csv");
        if (target == null) return;
        List<PreviewReport.StateEntry> entries = List.copyOf(preview.entries());
        runExport(target, () -> {
            try (BufferedWriter writer = Files.newBufferedWriter(target, StandardCharsets.UTF_8)) {
                writeCsv(writer, "stateName", "key", "mapKey", "value", "namespace", "ttlTimestamp", "keyGroup", "decodeStatus", "keyHex", "valueHex");
                for (PreviewReport.StateEntry entry : entries) writeCsv(writer, entry.stateName(), entry.key(), entry.mapKey(), entry.value(), entry.namespace(), entry.ttlTimestamp() == null ? "" : entry.ttlTimestamp().toString(), Integer.toString(entry.keyGroup()), entry.decodeStatus(), entry.keyHex(), entry.valueHex());
            }
        });
    }

    private Path exportTarget(String title, String fileName, String extension) {
        JFileChooser chooser = chooser(title);
        chooser.setSelectedFile(new java.io.File(fileName));
        if (chooser.showSaveDialog(this) != JFileChooser.APPROVE_OPTION) return null;
        Path path = chooser.getSelectedFile().toPath().toAbsolutePath();
        if (!path.getFileName().toString().toLowerCase(java.util.Locale.ROOT).endsWith(extension)) path = path.resolveSibling(path.getFileName() + extension);
        if (protectedSource(path)) {
            showFailure("不能覆盖原始状态文件", new IllegalArgumentException("请选择其他导出位置；原始元数据、状态文件和加载的 JAR 仅用于读取。"));
            return null;
        }
        if (Files.exists(path) && JOptionPane.showConfirmDialog(this, "文件已存在，是否覆盖？\n" + path, "确认覆盖", JOptionPane.OK_CANCEL_OPTION, JOptionPane.WARNING_MESSAGE) != JOptionPane.OK_OPTION) return null;
        return path;
    }

    private boolean protectedSource(Path target) {
        List<Path> sources = new ArrayList<>(loadedJars);
        if (report != null) {
            sources.add(report.metadataPath());
            for (SnapshotReport.StateFile file : report.files()) {
                if (file.localPath() != null && !file.localPath().isBlank()) {
                    try { sources.add(Path.of(file.localPath())); }
                    catch (RuntimeException ignored) { /* Embedded or unmapped handles have no local file. */ }
                }
            }
        }
        for (Path source : sources) {
            if (source.toAbsolutePath().normalize().equals(target.toAbsolutePath().normalize())) return true;
            try { if (Files.exists(target) && Files.exists(source) && Files.isSameFile(target, source)) return true; }
            catch (java.io.IOException ignored) { /* Normalized equality still protects directly referenced files. */ }
        }
        return false;
    }

    private void runExport(Path target, ExportAction action) {
        beginOperation("正在导出…");
        new SwingWorker<Void, Void>() {
            @Override protected Void doInBackground() throws Exception { action.run(); return null; }
            @Override protected void done() {
                try { get(); setStatus("已导出：" + target, "INFO"); }
                catch (Exception error) { showFailure("导出失败", unwrap(error)); }
                finally { endOperation(); }
            }
        }.execute();
    }

    private void beginOperation(String message) {
        beginOperation(message, false);
    }

    private void beginOperation(String message, boolean allowCancellation) {
        busy = true;
        cancellable = allowCancellation;
        cancellationRequested = false;
        progress.setVisible(true);
        progress.setIndeterminate(true);
        setStatus(message, "INFO");
        updateActions();
    }

    private void endOperation() {
        busy = false;
        cancellable = false;
        progress.setIndeterminate(false);
        progress.setVisible(false);
        updateActions();
        if (closing) finishClose();
    }

    private void updateActions() {
        boolean editable = !busy && !closing;
        snapshotPath.setEnabled(editable);
        browseSnapshot.setEnabled(editable);
        openSnapshot.setEnabled(editable);
        addMapping.setEnabled(editable);
        removeMapping.setEnabled(editable && mappingModel.getRowCount() > 0);
        mappings.setEnabled(editable);
        chooseJars.setEnabled(editable);
        clearJars.setEnabled(editable && !userJars.isEmpty());
        operatorTree.setEnabled(editable);
        limit.setEnabled(editable);
        readSamples.setEnabled(editable && session != null && selected != null && selected.subtask() >= 0);
        cancelOperation.setEnabled(busy && cancellable && !closing && !cancellationRequested);
        exportReport.setEnabled(editable && report != null);
        exportSamples.setEnabled(editable && preview != null && !preview.entries().isEmpty());
        queryStateName.setEnabled(editable);
        queryKind.setEnabled(editable);
        queryKeyType.setEnabled(editable);
        queryValueType.setEnabled(editable);
        queryMapKeyType.setEnabled(editable && queryKind.getSelectedItem() == LocalStreamQueryService.Kind.MAP);
        queryLimit.setEnabled(editable);
        runQuery.setEnabled(editable && session != null && selected != null);
    }

    private void requestClose() {
        if (closing) return;
        closing = true;
        updateActions();
        if (busy) {
            if (cancellable) cancelReading();
            setStatus("正在关闭 · 等待当前任务实际退出后释放状态资源…", "INFO");
        }
        else finishClose();
    }

    private void cancelReading() {
        if (!busy || !cancellable || cancellationRequested) return;
        synchronized (operationLock) {
            cancellationRequested = true;
            if (operationThread != null) operationThread.interrupt();
        }
        setStatus("已请求取消 · 等待读取任务和本地资源清理完成…", "INFO");
        updateActions();
    }

    private <T> T executeReading(Callable<T> action) throws Exception {
        synchronized (operationLock) {
            if (cancellationRequested) throw new InterruptedException("已请求取消");
            operationThread = Thread.currentThread();
        }
        try { return action.call(); }
        finally {
            synchronized (operationLock) {
                operationThread = null;
                Thread.interrupted();
            }
        }
    }

    private void finishClose() {
        SnapshotSession closingSession = session;
        session = null;
        busy = true;
        setStatus("正在释放本地状态资源…", "INFO");
        new SwingWorker<Void, Void>() {
            @Override protected Void doInBackground() throws Exception {
                if (closingSession != null) closingSession.close();
                return null;
            }
            @Override protected void done() {
                try { get(); }
                catch (Exception error) { System.err.println("关闭快照资源失败：" + unwrap(error)); }
                finally { dispose(); }
            }
        }.execute();
    }

    private static void closeSessionAsync(SnapshotSession orphan) {
        new SwingWorker<Void, Void>() {
            @Override protected Void doInBackground() throws Exception { orphan.close(); return null; }
            @Override protected void done() {
                try { get(); } catch (Exception error) { System.err.println("关闭过期快照失败：" + unwrap(error)); }
            }
        }.execute();
    }

    private void showFailure(String title, Throwable error) {
        String message = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
        setStatus(title + " · " + message, "ERROR");
        diagnosticsModel.addRow(new Object[]{"ERROR", title + "：" + message});
        if (!closing) {
            JTextArea text = new JTextArea(message, 5, 65);
            text.setEditable(false);
            text.setLineWrap(true);
            text.setWrapStyleWord(true);
            JOptionPane.showMessageDialog(this, new JScrollPane(text), title, JOptionPane.ERROR_MESSAGE);
        }
    }

    private void setStatus(String message, String severity) {
        status.setText(message);
        status.setToolTipText(message);
        status.setForeground("ERROR".equals(severity) ? new Color(174, 45, 55) : "WARN".equals(severity) ? new Color(150, 98, 14) : MUTED);
    }

    private JFileChooser chooser(String title) {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle(title);
        if (!snapshotPath.getText().isBlank()) {
            try {
                Path path = Path.of(snapshotPath.getText().trim());
                Path directory = Files.isDirectory(path) ? path : path.toAbsolutePath().getParent();
                if (directory != null && Files.isDirectory(directory)) chooser.setCurrentDirectory(directory.toFile());
            } catch (RuntimeException ignored) { /* Invalid input is reported on import. */ }
        }
        return chooser;
    }

    private static boolean hasError(SnapshotReport report) {
        return report.diagnostics().stream().anyMatch(d -> "ERROR".equalsIgnoreCase(d.severity()));
    }

    private static boolean hasDecodeStatus(PreviewReport.StateEntry entry, String status) {
        String value = Objects.toString(entry.decodeStatus(), "").toUpperCase(java.util.Locale.ROOT);
        return value.equals(status) || value.startsWith(status + ":");
    }

    private static String operatorLabel(SnapshotReport.OperatorReport operator) {
        if (operator.operatorName() != null && !operator.operatorName().isBlank()) return "算子 · " + operator.operatorName();
        if (operator.operatorUid() != null && !operator.operatorUid().isBlank()) return "UID · " + operator.operatorUid();
        return "未命名算子 · " + shortHash(operator.operatorId());
    }

    private static String operatorIdentity(SnapshotReport.OperatorReport operator) {
        return "<html>名称：" + html(identityField(operator.operatorName()))
                + "<br>UID：" + html(identityField(operator.operatorUid()))
                + "<br>Hash：" + html(operator.operatorId()) + "</html>";
    }

    private static String identityField(String value) {
        return value == null || value.isBlank() ? "未记录" : value;
    }

    private static String shortHash(String value) {
        return shortText(Objects.toString(value, ""), 8);
    }

    private static String shortText(String value, int maximum) {
        return value.length() <= maximum ? value : value.substring(0, maximum) + "…";
    }

    private static String html(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&#39;");
    }

    static String shortSnapshotKind(String kind) {
        if (kind == null || kind.isBlank() || "UNKNOWN".equalsIgnoreCase(kind.trim())) return "未记录";
        String value = kind.trim();
        java.util.regex.Matcher name = Pattern.compile("(?i)name\\s*=\\s*['\"]([^'\"]+)['\"]").matcher(value);
        if (name.find()) value = name.group(1);
        else if (value.toUpperCase(java.util.Locale.ROOT).contains("SAVEPOINT")) value = "Savepoint";
        else if (value.toUpperCase(java.util.Locale.ROOT).contains("CHECKPOINT")) value = "Checkpoint";
        return value.length() > 32 ? value.substring(0, 32) + "…" : value;
    }

    private static String availability(String value) {
        if (value == null) return "未知";
        return switch (value) {
            case "PRESENT" -> "可读取";
            case "EMBEDDED" -> "内嵌状态";
            case "MISSING" -> "文件缺失";
            case "SIZE_MISMATCH" -> "大小不符";
            case "UNMAPPED" -> "未映射 URI";
            case "REJECTED" -> "路径被拒绝";
            case "UNSUPPORTED" -> "暂不支持";
            case "UNREADABLE" -> "无法读取";
            default -> value;
        };
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof ExecutionException && error.getCause() != null ? error.getCause() : error;
    }

    private static JPanel card() {
        JPanel panel = new JPanel();
        panel.setBackground(Color.WHITE);
        panel.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(new Color(215, 225, 239)), new EmptyBorder(10, 12, 10, 12)));
        return panel;
    }

    private static JPanel metricCard(String label, JLabel value) {
        JPanel panel = card();
        panel.setLayout(new BorderLayout(0, 4));
        JLabel caption = new JLabel(label);
        caption.setForeground(MUTED);
        panel.add(caption, BorderLayout.NORTH);
        panel.add(value, BorderLayout.CENTER);
        return panel;
    }

    private static JLabel metricValue() {
        JLabel label = new JLabel("—");
        label.setFont(label.getFont().deriveFont(Font.BOLD, 19f));
        label.setForeground(INK);
        return label;
    }

    private static JComboBox<LocalStreamQueryService.ScalarType> scalarTypes() {
        return new JComboBox<>(LocalStreamQueryService.ScalarType.values());
    }

    private static DefaultTableModel readOnlyModel(String... columns) {
        return new DefaultTableModel(columns, 0) {
            @Override public boolean isCellEditable(int row, int column) { return false; }
        };
    }

    private static JTable table(DefaultTableModel model) {
        JTable table = new JTable(model) {
            @Override public String getToolTipText(java.awt.event.MouseEvent event) {
                int row = rowAtPoint(event.getPoint());
                int column = columnAtPoint(event.getPoint());
                if (row < 0 || column < 0) return null;
                String value = Objects.toString(getValueAt(row, column), "");
                return (value.length() > 350 ? value.substring(0, 350) + "…" : value) + "  （双击查看完整内容）";
            }
        };
        table.setRowHeight(27);
        table.setShowGrid(false);
        table.setIntercellSpacing(new Dimension(8, 1));
        table.setFillsViewportHeight(true);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.setAutoCreateRowSorter(true);
        table.getTableHeader().setReorderingAllowed(false);
        table.addMouseListener(new MouseAdapter() {
            @Override public void mouseClicked(MouseEvent event) {
                if (event.getClickCount() != 2 || !SwingUtilities.isLeftMouseButton(event)) return;
                int row = table.rowAtPoint(event.getPoint());
                int column = table.columnAtPoint(event.getPoint());
                if (row < 0 || column < 0) return;
                String value = Objects.toString(table.getValueAt(row, column), "");
                JTextArea text = new JTextArea(value, 18, 85);
                text.setEditable(false);
                text.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
                text.setCaretPosition(0);
                text.setTabSize(2);
                JButton copy = new JButton("复制全部内容");
                copy.addActionListener(e -> {
                    try {
                        Toolkit.getDefaultToolkit().getSystemClipboard().setContents(new java.awt.datatransfer.StringSelection(value), null);
                        copy.setText("已复制");
                    } catch (RuntimeException error) { copy.setText("复制失败，可选中文本复制"); }
                });
                JPanel detail = new JPanel(new BorderLayout(0, 8));
                detail.add(new JScrollPane(text), BorderLayout.CENTER);
                JPanel actions = new JPanel(new FlowLayout(FlowLayout.RIGHT, 0, 0));
                actions.add(copy);
                detail.add(actions, BorderLayout.SOUTH);
                JOptionPane.showMessageDialog(table, detail, table.getColumnName(column) + " · 只读详情", JOptionPane.PLAIN_MESSAGE);
            }
        });
        table.setToolTipText("双击单元格查看完整内容并复制。");
        return table;
    }

    static String bytes(long value) {
        if (value < 0) return "未知";
        if (value < 1024) return value + " B";
        String[] units = {"KiB", "MiB", "GiB", "TiB", "PiB"};
        double converted = value;
        int unit = -1;
        do { converted /= 1024; unit++; } while (converted >= 1024 && unit < units.length - 1);
        return String.format(java.util.Locale.ROOT, "%.2f %s", converted, units[unit]);
    }

    static void writeCsv(BufferedWriter writer, String... fields) throws java.io.IOException {
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) writer.write(',');
            writer.write('"');
            writer.write(Objects.toString(fields[i], "").replace("\"", "\"\""));
            writer.write('"');
        }
        writer.write("\r\n");
    }

    private record TreeItem(String operatorId, int subtask, String label, String identityTooltip) {
        @Override public String toString() { return label; }
    }

    public record ReportExport(String tool, SnapshotReport snapshot, String selectedOperatorId,
                               Integer selectedSubtask, PreviewReport boundedPreview) {}

    @FunctionalInterface private interface ExportAction { void run() throws Exception; }
}
