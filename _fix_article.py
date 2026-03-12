import re

def fix_article_html(filepath: str):
    """
    Fixes the duplicated renderContent and broken renderAttachments 
    logic found after the first meta.innerHTML assignment in article.html.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Normalize line endings
    content = content.replace('\r\n', '\n').replace('\r', '\n')

    # The marker where the corruption begins
    marker = r'        \)\.join\(""\) \+ \(ext \? `<div class="meta-item"><strong>원문 링크</strong><a href="\$\{TextUtil\.escapeHtml\(ext\)\}" target="_blank" rel="noopener noreferrer">원문 보기 →</a></div>` : ""\);'

    parts = re.split(marker, content)

    if len(parts) >= 3:
        # Pre-corruption content
        safe_prefix = parts[0]
        
        # The correct marker
        safe_marker = '        ).join("") + (ext ? `<div class="meta-item"><strong>원문 링크</strong><a href="${TextUtil.escapeHtml(ext)}" target="_blank" rel="noopener noreferrer">원문 보기 →</a></div>` : "");\n'
        
        # The correctly formatted replacement block
        correct_block = """
        this.renderContent(data);
        this.renderAttachments(data.attachments || []);
      }

      renderContent(data) {
        const htmlBody = TextUtil.decodeEntities(data.content_html || "");
        if (htmlBody.trim()) {
          this.el.content.classList.remove("plain");
          let inner = HtmlUtil.sanitize(htmlBody);
          // Compress 3+ <br> tags to 2
          inner = inner.replace(/(<br\\s*\\/?>\\s*){3,}/gi, "<br><br>");
          // Add spacer between 2 <br> tags
          inner = inner.replace(/(<br\\s*\\/?>)\\s*(<br\\s*\\/?>)/gi, '$1<span style="font-size:10px;display:block;line-height:1;"> </span>');
          // Compress multiple empty <p> tags
          inner = inner.replace(/(<p[^>]*>\\s*<\\/p>\\s*){2,}/gi, "<p></p>");
          
          this.el.content.innerHTML = inner;
          UrlUtil.normalizeEmbeddedUrls(this.el.content, data);
          return;
        }
        this.el.content.classList.add("plain");
        const rawText = TextUtil.decodeEntities(data.content_text || "(본문 없음)");
        this.el.content.textContent = rawText.replace(/\\n{3,}/g, "\\n\\n");
      }

      renderAttachments(items) {
        if (!items.length) {
          this.el.attList.innerHTML = `<li class="empty">첨부파일이 없습니다.</li>`;
          return;
        }
"""
        # Rest of the file safely extracted after the corruption
        rest = parts[-1]
        
        # Find where renderAttachments starts in the rest of the file
        # It's broken in the corrupted version (just a '}' block), so we search for standard elements inside attachments
        items_match = re.search(r'        this\.el\.attList\.innerHTML = items\.map', rest)
        if items_match:
            rest_clean = rest[items_match.start():]
            
            new_content = safe_prefix + safe_marker + correct_block + rest_clean
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
                
            print(f"Success! Fixed corrupted file. Lines: {new_content.count(chr(10))}")
        else:
            print("Failed to find attachment mapping logic in remainder.")
    elif len(parts) == 2:
        print("File is already clean. No duplicates found.")
    else:
        print(f"Unexpected structure. Found {len(parts)} parts.")

if __name__ == "__main__":
    fix_article_html(r'C:\Users\admin1\Documents\보도자료 테스트\article.html')

