package cellarium.parser.templates;

import java.util.regex.Pattern;

public final class QueryTemplates {
    //TODO: Сделать строже?
    public static final Pattern SELECT = Pattern.compile("(?i)^SELECT\\s+(VALUES\\(\\s*(?:.+)(?:\\s*,.+)*\\s*\\)|\\*)\\s+FROM\\s+(.+)+\\s*;$");
    public static final Pattern SELECT_WHERE = Pattern.compile("(?i)^SELECT\\s+(VALUES\\(\\s*(?:.+)(?:\\s*,.+)*\\s*\\)|\\*)\\s+FROM\\s+(.+)+\\s+WHERE\\s+(.+)\\s*=\\s*(.+)\\s*;$");
    public static final Pattern INSERT = Pattern.compile("(?i)^INSERT\\s+INTO\\s+(.+)+\\s+\\((\\s*(?:.+)(?:\\s*,.+)*\\s*)\\)\\s+VALUES\\s+\\((\\s*(?:.+)(?:\\s*,.+)*\\s*)\\)\\s*;$");
    public static final Pattern DELETE = Pattern.compile("(?i)^DELETE\\s+FROM\\s+(.+)+\\s+WHERE\\s+(.+)\\s*=\\s*(.+)\\s*;$");


    public static final Pattern DROP_TABLE = Pattern.compile("(?i)^DROP\\s+TABLE\\s+(.+)+\\s*;$");

    private QueryTemplates() {}
}
