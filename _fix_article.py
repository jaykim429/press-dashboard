filepath = r'C:\Users\admin1\Documents\보도자료 테스트\article.html'

with open(filepath, 'rb') as f:
    raw = f.read().decode('utf-8')

# Normalize line endings to \n
content = raw.replace('\r\n', '\n').replace('\r', '\n')

# ---- The problematic duplicated block starts right after line 481
# We need to cut from the duplicate showError at line 483 down to and including
# the broken renderAttachments start, and replace renderAttachments correctly.

# Find the first occurrence of the correctly-placed showError (after render ends at ~482)
# Strategy: find the double-insertion and remove it cleanly.

# The correct render() ends with this line:
correct_render_end = '        ).join("") + (ext ? `<div class="meta-item"><strong>원문 링크</strong><a href="${TextUtil.escapeHtml(ext)}" target="_blank" rel="noopener noreferrer">원문 보기 →</a></div>` : "");\n'

# Then the FIRST occurrence is the real one (original), then what follows should be:
# this.renderContent(data); this.renderAttachments(data.attachments || []); }
# But instead we got a duplicate block. Let's find the duplicate and fix.

# Approach: split on the meta.innerHTML line to get the two copies
parts = content.split('        ).join("") + (ext ? `<div class="meta-item"><strong>원문 링크</strong><a href="${TextUtil.escapeHtml(ext)}" target="_blank" rel="noopener noreferrer">원문 보기 →</a></div>` : "");')

print(f"Found {len(parts)} parts after split")
# parts[0] = everything before first meta.innerHTML end
# parts[1] = stuff between first and second (the duplicate block)  
# parts[2] = everything after second (the rest of the file)

if len(parts) == 3:
    # Reconstruct correctly:
    # part[0] + correct meta.innerHTML close
    # + the correct renderContent + renderAttachments
    # + part[2] which starts roughly from renderLinkedReport
    
    # What should come right after the first meta.innerHTML close is:
    correct_after = '''

        this.renderContent(data);
        this.renderAttachments(data.attachments || []);
      }

      renderContent(data) {
        const htmlBody = TextUtil.decodeEntities(data.content_html || "");
        if (htmlBody.trim()) {
          this.el.content.classList.remove("plain");
          this.el.content.innerHTML = HtmlUtil.sanitize(htmlBody);
          UrlUtil.normalizeEmbeddedUrls(this.el.content, data);
          // 3개 이상 연속 <br> -> 2개로 압축
          let inner = this.el.content.innerHTML;
          inner = inner.replace(/(\\s*<br\\s*\\/?>\\s*){3,}/gi, "<br><br>");
          // 2개 연속 <br> -> <br> + 좁은 간격 스페이서
          inner = inner.replace(/(<br\\s*\\/?>)\\s*(<br\\s*\\/?>)/gi,
            '$1<span style="font-size:10px;display:block;line-height:1;"> </span>');
          // 연속 빈 <p> 2개 이상 -> 1개로
          inner = inner.replace(/(<p[^>]*>\\s*<\\/p>\\s*){2,}/gi, "<p></p>");
          this.el.content.innerHTML = inner;
          return;
        }
        this.el.content.classList.add("plain");
        const rawText = TextUtil.decodeEntities(data.content_text || "(본문 없음)");
        // 3줄 이상 빈 줄 -> 최대 2줄로 압축
        this.el.content.textContent = rawText.replace(/\\n{3,}/g, "\\n\\n");
      }

'''
    
    # parts[2] starts with the second meta.innerHTML close then the duplicate showError+render+renderContent garbage
    # We need to skip all that and find renderAttachments
    rest = '        ).join("") + (ext ? `<div class="meta-item"><strong>원문 링크</strong><a href="${TextUtil.escapeHtml(ext)}" target="_blank" rel="noopener noreferrer">원문 보기 →</a></div>` : "");' + parts[2]
    
    # Find renderAttachments in the rest
    ra_marker = '      renderAttachments(items) {'
    ra_idx = rest.find(ra_marker)
    if ra_idx == -1:
        # try to find in parts[1] then parts[2]
        print("renderAttachments not found in parts[2]")
        # look in parts[1]
        ra_idx2 = parts[1].find(ra_marker)
        print(f"In parts[1] at: {ra_idx2}")
    else:
        rest_from_ra = rest[ra_idx:]
        
        # Fix broken renderAttachments: it starts with just "}" which is wrong
        # Current: renderAttachments(items) {\n        }\n  this.el.attList...
        broken_ra_start = '      renderAttachments(items) {\n        }\r\n'
        correct_ra_start = '      renderAttachments(items) {\n        if (!items.length) {\n          this.el.attList.innerHTML = `<li class="empty">첨부파일이 없습니다.</li>`;\n          return;\n        }\n'
        rest_from_ra = rest_from_ra.replace(broken_ra_start, correct_ra_start, 1)
        
        new_content = parts[0] + '        ).join("") + (ext ? `<div class="meta-item"><strong>원문 링크</strong><a href="${TextUtil.escapeHtml(ext)}" target="_blank" rel="noopener noreferrer">원문 보기 →</a></div>` : "");' + correct_after + rest_from_ra
        
        with open(filepath, 'wb') as f:
            f.write(new_content.encode('utf-8'))
        print("Success! Lines:", new_content.count('\n'))
else:
    print("Unexpected number of parts:", len(parts))
    for i, p in enumerate(parts):
        print(f"Part {i} length: {len(p)}")
