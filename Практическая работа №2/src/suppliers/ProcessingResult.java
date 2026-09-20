package suppliers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Итог обработки одного файла. */
public final class ProcessingResult {
    public final String fileName;
    public long size;
    public String sha256 = "";
    public String status = "";
    public int validRows;
    public int invalidRows;
    public BigDecimal totalValue = BigDecimal.ZERO;
    public String readMethod = "";
    public Map<String, BigDecimal> totalBySupplier = new LinkedHashMap<String, BigDecimal>();
    public List<String> errors = new ArrayList<String>();

    public ProcessingResult(String fileName) {
        this.fileName = fileName;
    }

    public String format() {
        StringBuilder sb = new StringBuilder();
        sb.append(fileName).append('\n');
        sb.append("Size: ").append(size).append(" bytes\n");
        sb.append("SHA-256: ").append(sha256).append('\n');
        sb.append("Read via: ").append(readMethod).append('\n');
        sb.append("Rows: valid=").append(validRows).append(", invalid=").append(invalidRows).append('\n');
        for (String e : errors) {
            sb.append("  ! ").append(e).append('\n');
        }
        for (Map.Entry<String, BigDecimal> e : totalBySupplier.entrySet()) {
            sb.append("  ").append(e.getKey()).append(": ").append(e.getValue().toPlainString()).append('\n');
        }
        sb.append("Total value: ").append(totalValue.toPlainString()).append('\n');
        sb.append("Status: ").append(status);
        return sb.toString();
    }
}
