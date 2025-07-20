document.addEventListener("DOMContentLoaded", function () {
    // Khai báo và khởi tạo các biến
    const elements = {
        searchForm: document.getElementById('search-form'),
        queryInput: document.getElementById('text-query'),
        imageInput: document.getElementById('image-upload'),
    };

    let allFrames = [];
    let allMetadata = [];
    let searchType = 'image';

    // Xử lý sự kiện tìm kiếm
    elements.searchForm.addEventListener('submit', handleSearch);
    elements.queryInput.addEventListener('keypress', handleEnterKey);

    function handleSearch(event) {
        event.preventDefault();
        const hasImage = elements.imageInput && elements.imageInput.files && elements.imageInput.files.length > 0;
        const hasText = elements.queryInput.value.trim() !== '';
        if (hasImage) {
            performImageSearch();
        } else if (hasText) {
            performSearch();
        } else {
            displayError('Please enter a query or upload an image.');
        }
    }

    function handleEnterKey(event) {
        if (event.key === 'Enter') {
            event.preventDefault();
            performSearch();
        }
    }

    function performSearch() {
        const formData = new FormData(elements.searchForm);
        const queryValue = elements.queryInput.value.trim();
        console.log("Performing search with query:", queryValue);

        if (!queryValue) {
            displayError('Please enter a query.');
            return;
        }

        formData.append('query', queryValue);

        fetch('/search', {
            method: 'POST',
            body: formData
        })
        .then(response => {
            console.log("Received response:", response.status);
            return response.json();
        })
        .then(data => {
            console.log("Received data:", data);
            displaySearchResults(data);
        })
        .catch(error => {
            console.error('Error:', error);
            displayError('An error occurred while processing your request. Please try again.');
        });
    }

    function performImageSearch() {
        const formData = new FormData(elements.searchForm);
        const file = elements.imageInput.files[0];
        if (!file) {
            displayError('Please select an image.');
            return;
        }
        formData.append('image', file);
        fetch('/search_image', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            displaySearchResults(data);
        })
        .catch(error => {
            console.error('Error:', error);
            displayError('An error occurred while processing your request. Please try again.');
        });
    }

    //Hiển thị kết quả tìm kiếm
    function displaySearchResults(data) {
        console.log("Starting to display search results");
        const searchResultsContainer = document.getElementById('search-results');
        if (!searchResultsContainer) {
            console.error('searchResultsContainer element not found.');
            return;
        }
        searchResultsContainer.innerHTML = '';
    
        if (data.error) {
            console.error("Error in response:", data.error);
            displayError(data.error);
            return;
        }
    
        const images = data.image_paths || [];
        const metadata = data.metadata_list || [];
        console.log("Processing results - Images:", images.length, "Metadata:", metadata.length);
    
        if (images.length > 0) {
            images.forEach((imagePath, index) => {
                console.log(`Creating container for image ${index + 1}:`, imagePath);
                const container = document.createElement('div');
                container.className = 'product-container';
        
                const img = document.createElement('img');
                img.src = imagePath;
                img.alt = metadata[index].name;
                img.className = 'product-image';
                
                // Add load and error event listeners to track image loading
                img.addEventListener('load', () => {
                    console.log(`Image ${index + 1} loaded successfully:`, imagePath);
                });
                img.addEventListener('error', () => {
                    console.error(`Failed to load image ${index + 1}:`, imagePath);
                });
                
                img.addEventListener('click', () => {
                    window.open(metadata[index].short_url, '_blank');
                });
                
                const productInfo = document.createElement('div');
                productInfo.className = 'product-info';
                productInfo.innerHTML = `
                    <h3 class="product-name">${metadata[index].name}</h3>
                    <div class="price-container">
                        <p class="price">₫${Number(metadata[index].price).toLocaleString()}</p>
                        ${metadata[index].discount_percentage > 0 ? 
                            `<p class="original-price">₫${Number(metadata[index].original_price).toLocaleString()}</p>
                             <span class="discount">-${metadata[index].discount_percentage}%</span>` 
                            : ''}
                    </div>
                `;
                
                const infoButton = document.createElement('button');
                infoButton.className = 'info-button';
                infoButton.textContent = 'Details';
                infoButton.addEventListener('click', () => showInfo(metadata[index]));
                
                container.appendChild(img);
                container.appendChild(productInfo);
                container.appendChild(infoButton);
                
                searchResultsContainer.appendChild(container);
                console.log(`Container ${index + 1} added to results`);
            });
        } else {
            console.log("No images found in results");
            searchResultsContainer.innerHTML = '<p class="no-results">No products found. Try a different query.</p>';
        }
    }
    
    //hiển thị thông tin chi tiết của ảnh
    function showInfo(metadata) {
        const infoDetails = document.getElementById('info-details');
        const infoContent = `
            <h2 class="product-title">${metadata.name}</h2>
            <div class="product-image-container">
                <img src="${metadata.image_path}" alt="${metadata.name}" class="product-detail-image">
            </div>
            <div class="product-details">
                <div class="price-details">
                    <p class="current-price"><strong>Price:</strong> ₫${Number(metadata.price).toLocaleString()}</p>
                    ${metadata.discount_percentage > 0 ? 
                        `<p class="original-price"><strong>Original Price:</strong> ₫${Number(metadata.original_price).toLocaleString()}</p>
                         <p class="discount-info"><strong>Discount:</strong> ${metadata.discount_percentage}%</p>` 
                        : ''}
                </div>
                <div class="description">
                    <p><strong>Description:</strong></p>
                    <p>${metadata.description}</p>
                </div>
                <a href="${metadata.short_url}" target="_blank" class="view-store-btn">View on Store</a>
            </div>
        `;
        
        infoDetails.innerHTML = infoContent;
        document.getElementById('info-modal').style.display = 'block';
    }

    //hiển thị thông báo lỗi
    function displayError(message) {
        const searchResultsContainer = document.getElementById('search-results');
        if (searchResultsContainer) {
            searchResultsContainer.innerHTML = `<p class="error-message">${message}</p>`;
        }
    }

    window.closeInfoModal = function() {
        const infoModal = document.getElementById('info-modal');
        if (infoModal) infoModal.style.display = 'none';
    };

    // Close modal when clicking outside
    window.onclick = function(event) {
        const modal = document.getElementById('info-modal');
        if (event.target === modal) {
            closeInfoModal();
        }
    };
});