# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Array Viewer</title>
    <style>
        .container { max-width: 1200px; margin: 0 auto; }
        .slider { width: 100%; margin: 20px 0; }
        .grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; }
        .array-container { text-align: center; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Array Viewer</h1>
        <input type="range" min="5" max="50" value="10" class="slider" id="sizeSlider">
        <div>Array size: <span id="sizeValue">10x10</span></div>
        <div class="grid" id="arrayGrid"></div>
    </div>
    <script>
        function updateImages() {
            fetch('/get_images')
                .then(response => response.json())
                .then(data => {
                    const grid = document.getElementById('arrayGrid');
                    grid.innerHTML = '';
                    for (const [topic, imageData] of Object.entries(data)) {
                        const container = document.createElement('div');
                        container.className = 'array-container';
                        container.innerHTML = `
                            <h3>${topic}</h3>
                            <img src="data:image/png;base64,${imageData}" width="400"
                            height="400">
                        `;
                        grid.appendChild(container);
                    }
                });
        }

        document.getElementById('sizeSlider').oninput = function() {
            let size = parseInt(this.value);
            document.getElementById('sizeValue').textContent = size + 'x' + size;
            fetch('/update_size/' + size, {method: 'POST'});
        };

        setInterval(updateImages, 1000);
    </script>
</body>
</html>
'''
