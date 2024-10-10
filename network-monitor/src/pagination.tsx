interface PaginationInterface {
    currentPage: number;
    totalPages: number;
    onPageChange: (page: number) => void;
}

export const Pagination = ({ currentPage, totalPages, onPageChange }: PaginationInterface) => {

    const render_all_pages = () => {
        let return_values = [];
    
        let start = currentPage - 2;

        if (start < 1) {
            start = 1;
        }

        let end = start + 4;

        if (end > totalPages) {
            end = totalPages;
        }
    
        if (start > 1) {
            return_values.push(
                <li key={1}>
                    <button className={`button pagination-link ${currentPage == 1 && 'is-current'}`} aria-label={`Goto page 1`} onClick={() => onPageChange(1)}>1</button>
                </li>
            );
        }


        for (let i = start; i <= end; i++) {
            return_values.push(
                <li key={i}>
                    <button className={`button pagination-link ${currentPage == i && 'is-current'}`} aria-label={`Goto page ${i}`} onClick={() => onPageChange(i)}>{i}</button>
                </li>
            );
        }
    
        if (end < totalPages) {

            return_values.push(
                <li key={totalPages}>
                    <button className={`button pagination-link ${currentPage == totalPages && 'is-current'}`} aria-label={`Goto page ${totalPages}`} onClick={() => onPageChange(totalPages)}>{totalPages}</button>
                </li>
            );
        }

        return return_values;
    }
    
    return (
    <nav className="pagination is-centered" role="navigation" aria-label="pagination">
    <button className={`button pagination-previous ${currentPage < 2 && 'is-disabled'}`} disabled={currentPage < 2} onClick={() => currentPage > 1 && onPageChange(currentPage - 1)}>Previous</button>
    <button className={`button pagination-previous `} disabled={currentPage == totalPages} onClick={() => currentPage != totalPages && onPageChange(currentPage + 1)}>Next page</button>
      <ul className="pagination-list is-justify-content-start">

      {render_all_pages()}

      </ul>
    </nav>
    );

}   
