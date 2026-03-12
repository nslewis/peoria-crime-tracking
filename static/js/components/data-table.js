export function renderDataTable(container, columns, rows, options = {}) {
    const wrap = document.createElement('div');
    wrap.className = 'data-table-wrap';

    let sortCol = null;
    let sortDir = 'asc';
    let sortedRows = [...rows];

    function buildTable() {
        const table = document.createElement('table');
        table.className = 'data-table';

        // Header
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        for (const col of columns) {
            const th = document.createElement('th');
            th.textContent = col.label;
            if (sortCol === col.key) {
                th.classList.add(sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
            }
            th.addEventListener('click', () => {
                if (sortCol === col.key) {
                    sortDir = sortDir === 'asc' ? 'desc' : 'asc';
                } else {
                    sortCol = col.key;
                    sortDir = 'asc';
                }
                sortedRows.sort((a, b) => {
                    const va = a[col.key] ?? '';
                    const vb = b[col.key] ?? '';
                    if (typeof va === 'number' && typeof vb === 'number') {
                        return sortDir === 'asc' ? va - vb : vb - va;
                    }
                    return sortDir === 'asc'
                        ? String(va).localeCompare(String(vb))
                        : String(vb).localeCompare(String(va));
                });
                buildTable();
            });
            headerRow.appendChild(th);
        }
        thead.appendChild(headerRow);
        table.appendChild(thead);

        // Body
        const tbody = document.createElement('tbody');
        for (const row of sortedRows) {
            const tr = document.createElement('tr');
            for (const col of columns) {
                const td = document.createElement('td');
                const val = row[col.key];
                if (col.render) {
                    td.innerHTML = col.render(val, row);
                } else {
                    td.textContent = val ?? '';
                }
                if (col.mono) td.classList.add('mono');
                tr.appendChild(td);
            }
            tbody.appendChild(tr);
        }
        table.appendChild(tbody);

        wrap.innerHTML = '';
        wrap.appendChild(table);
    }

    buildTable();
    container.appendChild(wrap);

    // CSV export
    if (options.exportable !== false && rows.length > 0) {
        const btn = document.createElement('button');
        btn.className = 'btn btn-sm';
        btn.style.marginTop = '12px';
        btn.textContent = 'Export CSV';
        btn.addEventListener('click', () => exportCSV(columns, rows, options.filename || 'data'));
        container.appendChild(btn);
    }
}

function exportCSV(columns, rows, filename) {
    const header = columns.map(c => c.label).join(',');
    const body = rows.map(r =>
        columns.map(c => {
            const v = r[c.key] ?? '';
            return `"${String(v).replace(/"/g, '""')}"`;
        }).join(',')
    ).join('\n');
    const blob = new Blob([header + '\n' + body], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${filename}.csv`;
    a.click();
    URL.revokeObjectURL(url);
}
