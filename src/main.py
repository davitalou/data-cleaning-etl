from src.extract import read_kpi_file, read_modeling_sheet
from src.transform import (
    transform_kpi,
    transform_khach_hang,
    transform_san_pham,
    transform_nhan_vien,
    transform_chi_nhanh,
    transform_du_lieu_ban_hang,
)
from src.load import (
    truncate_table,
    insert_kpi,
    merge_khach_hang,
    merge_san_pham,
    merge_nhan_vien,
    merge_chi_nhanh,
    load_du_lieu_ban_hang,
)

def run_etl():
    print("=== BAT DAU ETL ===")

    # 1. KPI_TEMPLATE
    print("Đọc KPI file...")
    kpi_df = read_kpi_file()
    kpi_df = transform_kpi(kpi_df)

    print("Load KPI_TEMPLATE...")
    truncate_table("KPI_TEMPLATE")
    insert_kpi(kpi_df)

    # 2. Các bảng dimension
    print("Đọc sheet KhachHang...")
    kh_df = transform_khach_hang(read_modeling_sheet("Khách hàng"))
    merge_khach_hang(kh_df)

    print("Đọc sheet SanPham...")
    sp_df = transform_san_pham(read_modeling_sheet("Sản phẩm"))
    merge_san_pham(sp_df)

    print("Đọc sheet NhanVien...")
    nv_df = transform_nhan_vien(read_modeling_sheet("Nhân viên"))
    merge_nhan_vien(nv_df)

    print("Đọc sheet ChiNhanh...")
    cn_df = transform_chi_nhanh(read_modeling_sheet("Chi nhánh"))
    merge_chi_nhanh(cn_df)

    # 3. Fact table
    print("Đọc sheet DuLieuBanHang...")
    bh_df = transform_du_lieu_ban_hang(read_modeling_sheet("Dữ liệu bán hàng"))

    print("Load DU_LIEU_BAN_HANG...")
    truncate_table("DU_LIEU_BAN_HANG")
    load_du_lieu_ban_hang(bh_df)

    print("=== ETL HOAN TAT ===")